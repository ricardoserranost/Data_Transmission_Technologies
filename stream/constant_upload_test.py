import argparse, concurrent.futures as cf, csv, json, os, time
from time import perf_counter
from datetime import datetime
from pathlib import Path
import psutil
from google.cloud import storage
import numpy as np
import threading
from collections import deque
from queue import Queue, Full, Empty
import cv2
from io import BytesIO

# ============================
# System monitoring
# ============================
def monitor_system(interval_s: float, outfile: str, nic: str | None, stop_evt: threading.Event):
    with open(outfile, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts","cpu_percent","ram_percent","bytes_sent","bytes_recv","packets_sent","packets_recv","nic"])
        psutil.cpu_percent(interval=None)  # prime CPU measurement
        while not stop_evt.is_set():
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory().percent
            if nic:
                ni = psutil.net_io_counters(pernic=True).get(nic)
                if ni is None:
                    ni = psutil.net_io_counters(pernic=False)
            else:
                ni = psutil.net_io_counters(pernic=False)
            w.writerow([datetime.now().isoformat(), cpu, ram, ni.bytes_sent, ni.bytes_recv, ni.packets_sent, ni.packets_recv, nic or "aggregate"])
            f.flush()
            stop_evt.wait(interval_s)

# ============================
# Upload one frame (in-memory)
# ============================
def upload_one_bytes(client: storage.Client, bucket_name: str, data: bytes, dest_prefix: str, retries: int, timeout_s: int):
    bucket = client.bucket(bucket_name)
    blob_name = f"{dest_prefix}/{datetime.utcnow().isoformat()}.jpg"
    blob = bucket.blob(blob_name)
    attempt, status, error = 0, "ok", ""
    start = perf_counter()
    while True:
        try:
            blob.upload_from_file(BytesIO(data), timeout=timeout_s, content_type="image/jpeg")
            break
        except Exception as e:
            attempt += 1
            if attempt > retries:
                status = "fail"
                error = f"{type(e).__name__}: {e}"
                break
            time.sleep(min(2 ** attempt, 10))  # backoff
    dur = perf_counter() - start
    return {
        "blob": blob_name,
        "size_bytes": len(data),
        "duration_s": dur,
        "retries": attempt,
        "status": status,
        "error": error,
    }

# ============================
# Main
# ============================
def main():
    ap = argparse.ArgumentParser(description="Stream webcam frames to GCS with monitoring.")
    ap.add_argument("--bucket", required=True, help="GCS bucket name (without gs://)")
    ap.add_argument("--prefix", default="stream", help="Destination prefix in the bucket")
    ap.add_argument("--concurrency", type=int, default=4, help="Parallel uploads")
    ap.add_argument("--retries", type=int, default=3, help="Upload retries per frame")
    ap.add_argument("--timeout-s", type=int, default=60, help="Per-frame upload timeout")
    ap.add_argument("--sys-interval", type=float, default=1.0, help="System metrics sampling interval (s)")
    ap.add_argument("--nic", default=None, help="Optional NIC/interface to monitor. Defaults to aggregate.")
    ap.add_argument("--outdir", default="results_stream", help="Output folder")
    ap.add_argument("--init-fps", type=int, default=5, help="Initial target FPS")
    ap.add_argument("--min-fps", type=int, default=1, help="Minimum FPS")
    ap.add_argument("--max-fps", type=int, default=30, help="Maximum FPS")
    ap.add_argument("--max-seconds", type=int, default=300, help="Max streaming time (s)")
    ap.add_argument("--max-mb", type=int, default=500, help="Max total upload (MB)")
    ap.add_argument("--queue-size", type=int, default=20, help="Max for streaming queue")
    ap.add_argument("--drop-resolution", type=int, default=0, help="Set to 1 to drop all images to 720p")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    client = storage.Client()

    # Start monitoring
    stop_evt = threading.Event()
    mon_thr = threading.Thread(
        target=monitor_system, 
        args=(args.sys_interval, str(outdir / "sys_metrics.csv"), args.nic, stop_evt),
        daemon=True
    )
    mon_thr.start()

    # Open webcam
    cap = cv2.VideoCapture(0)
    if not cap.isOpened():
        raise SystemExit("Could not open webcam")
    
    if args.drop_resolution==1:
        cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1280)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 720)

    frame_queue = Queue(maxsize=args.queue_size)
    rows = []
    sent_bytes = 0
    start_time = time.time()
    fps = args.init_fps

    recent_lock = threading.Lock()
    recent_uploads = deque(maxlen=20)

    def uploader_worker():
        nonlocal sent_bytes
        while not stop_evt.is_set() or not frame_queue.empty():
            try:
                data = frame_queue.get(timeout=0.5)
            except Empty:
                continue
            r = upload_one_bytes(client, args.bucket, data, args.prefix, args.retries, args.timeout_s)
            rows.append(r)
            sent_bytes += r["size_bytes"]
            frame_queue.task_done()
            with recent_lock:
                recent_uploads.append(r["duration_s"])
            print(f"Frame: {r['status']} {r['duration_s']:.2f}s, retries={r['retries']}")

    # Start uploader threads
    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        for _ in range(args.concurrency):
            ex.submit(uploader_worker)

        try:
            while True:
                loop_start = time.perf_counter()
                ret, frame = cap.read()
                if not ret:
                    print("Frame capture failed, stopping.")
                    break

                ok, buf = cv2.imencode(".jpg", frame)
                if not ok:
                    continue
                data = buf.tobytes()

                try:
                    frame_queue.put_nowait(data)
                except Full:
                    print("Queue full, dropping frame")

                # Adjust FPS based on queue occupancy
                qsize = frame_queue.qsize()
                if qsize > args.queue_size * 0.8 and fps > args.min_fps:
                    fps -= 1
                    print("New FPS: ", fps)
                elif qsize < args.queue_size * 0.2 and fps < args.max_fps:
                    fps += 1
                    print("New FPS: ", fps)
                interval = 1.0 / fps

                # Stop conditions
                if (time.time() - start_time) > args.max_seconds:
                    print("Reached time limit, stopping.")
                    break
                if sent_bytes > args.max_mb * 1024 * 1024:
                    print("Reached size limit, stopping.")
                    break

                elapsed = time.perf_counter() - loop_start
                if elapsed < interval:
                    time.sleep(interval - elapsed)
        finally:
            cap.release()
            stop_evt.set()
            mon_thr.join(timeout=2)

    # Write logs
    with open(outdir / "uploads_log.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["blob","size_bytes","duration_s","retries","status","error"])
        w.writeheader()
        w.writerows(rows)

    sizes = np.array([r["size_bytes"] for r in rows if r["status"] == "ok"], dtype=float)
    durs  = np.array([r["duration_s"] for r in rows if r["status"] == "ok"], dtype=float)
    total_bytes = float(sizes.sum()) if sizes.size else 0.0
    wall = time.time() - start_time
    agg_mbps = (total_bytes * 8.0) / wall / 1e6 if wall > 0 else 0.0

    def pct(a, p): 
        return float(np.percentile(a, p)) if a.size else None

    summary = {
        "frames_total": len(rows),
        "bytes_total": int(total_bytes),
        "wall_seconds": wall,
        "throughput_mbps_wall": agg_mbps,
        "per_frame_latency_s": {
            "p50": pct(durs, 50),
            "p90": pct(durs, 90),
            "p95": pct(durs, 95),
            "p99": pct(durs, 99),
        },
        "concurrency": args.concurrency,
        "prefix": args.prefix,
        "nic": args.nic or "aggregate",
        "sys_interval": args.sys_interval,
        "timestamp": datetime.utcnow().isoformat()+"Z",
    }

    with open(outdir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))
    print(f"\nWrote results in {outdir}:\n - uploads_log.csv, sys_metrics.csv, summary.json")


if __name__ == "__main__":
    main()
