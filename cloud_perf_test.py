#!/usr/bin/env python3
import argparse, concurrent.futures as cf, csv, json, os, time
from time import perf_counter
from datetime import datetime
from pathlib import Path
import psutil
from google.cloud import storage
import numpy as np
import threading

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

def upload_one(client: storage.Client, bucket_name: str, path: str, dest_prefix: str, chunk_bytes: int | None, retries: int, timeout_s: int):
    bucket = client.bucket(bucket_name)
    blob_name = f"{dest_prefix}/{os.path.basename(path)}" if dest_prefix else os.path.basename(path)
    blob = bucket.blob(blob_name)
    if chunk_bytes:
        blob.chunk_size = chunk_bytes  # must be multiple of 256 KiB
    size = os.path.getsize(path)
    attempt = 0
    start = perf_counter()
    status = "ok"
    error = ""
    while True:
        try:
            blob.upload_from_filename(path, timeout=timeout_s)
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
        "file": path,
        "blob": blob_name,
        "size_bytes": size,
        "duration_s": dur,
        "retries": attempt,
        "status": status,
        "error": error,
    }

def main():
    ap = argparse.ArgumentParser(description="Measure Google Cloud Storage upload performance for a folder of files.")
    ap.add_argument("--folder", required=True, help="Folder containing files to upload")
    ap.add_argument("--bucket", required=True, help="GCS bucket name (without gs://)")
    ap.add_argument("--prefix", default="", help="Destination prefix in the bucket, e.g. 'tests/wifi'")
    ap.add_argument("--concurrency", type=int, default=8, help="Parallel uploads")
    ap.add_argument("--chunk-mb", type=int, default=32, help="Chunk size for resumable uploads (MiB). Must be multiple of 0.25 MiB.")
    ap.add_argument("--retries", type=int, default=3, help="Upload retries per file")
    ap.add_argument("--timeout-s", type=int, default=600, help="Per-file upload timeout")
    ap.add_argument("--sys-interval", type=float, default=1.0, help="System metrics sampling interval (seconds)")
    ap.add_argument("--nic", default=None, help="Optional NIC/interface name to monitor (e.g., 'wlan0', 'Ethernet'). Defaults to aggregate.")
    ap.add_argument("--outdir", default="results", help="Folder where output files will be written")
    args = ap.parse_args()

    # Ensure output directory exists
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    folder = Path(args.folder)
    files = sorted([str(p) for p in folder.iterdir() if p.is_file()])
    if not files:
        raise SystemExit(f"No files found in {folder}")

    chunk_bytes = int(args.chunk_mb * 1024 * 1024)
    if chunk_bytes % (256 * 1024) != 0:
        raise SystemExit("--chunk-mb must be a multiple of 0.25 MiB (256 KiB)")

    client = storage.Client()

    stop_evt = threading.Event()
    mon_thr = threading.Thread(
        target=monitor_system, 
        args=(args.sys_interval, str(outdir / "sys_metrics.csv"), args.nic, stop_evt), 
        daemon=True
    )
    mon_thr.start()

    t0 = perf_counter()
    print("Starting monitoring time: ", t0)
    rows, failed = [], 0

    with cf.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        print("Starting sending time: ", perf_counter())
        futs = [ex.submit(upload_one, client, args.bucket, f, args.prefix, chunk_bytes, args.retries, args.timeout_s) for f in files]
        for fut in cf.as_completed(futs):
            r = fut.result()
            rows.append(r)
            if r["status"] != "ok":
                failed += 1
            print(f"{os.path.basename(r['file'])}: {r['status']} {r['duration_s']:.2f}s, retries={r['retries']}")

    wall = perf_counter() - t0
    stop_evt.set()
    mon_thr.join(timeout=2)

    # To ensure a final measurement, after everything has been sent:
    # (That way the total bytes counter is accurate)
    with open(outdir / "sys_metrics.csv", "a", newline="") as f:
        w = csv.writer(f)
        ni = psutil.net_io_counters(pernic=True).get(args.nic) if args.nic else psutil.net_io_counters(pernic=False)
        cpu = psutil.cpu_percent(interval=None)
        ram = psutil.virtual_memory().percent
        w.writerow([datetime.now().isoformat(), cpu, ram, ni.bytes_sent, ni.bytes_recv, ni.packets_sent, ni.packets_recv, args.nic or "aggregate"])

    with open(outdir / "uploads_log.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["file","blob","size_bytes","duration_s","retries","status","error"])
        w.writeheader()
        w.writerows(rows)


    sizes = np.array([r["size_bytes"] for r in rows if r["status"] == "ok"], dtype=float)
    durs  = np.array([r["duration_s"] for r in rows if r["status"] == "ok"], dtype=float)
    total_bytes = float(sizes.sum()) if sizes.size else 0.0
    agg_mbps = (total_bytes * 8.0) / wall / 1e6 if wall > 0 else 0.0

    def pct(a, p): 
        return float(np.percentile(a, p)) if a.size else None

    summary = {
        "files_total": len(rows),
        "files_failed": failed,
        "error_rate": failed / len(rows) if rows else 0.0,
        "bytes_total": int(total_bytes),
        "wall_seconds": wall,
        "throughput_mbps_wall": agg_mbps,
        "per_file_latency_s": {
            "p50": pct(durs, 50),
            "p90": pct(durs, 90),
            "p95": pct(durs, 95),
            "p99": pct(durs, 99),
        },
        "concurrency": args.concurrency,
        "chunk_mb": args.chunk_mb,
        "retries": args.retries,
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
