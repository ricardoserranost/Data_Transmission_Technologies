# Webcam Stream to GCS

This script streams frames from a local **webcam** to a **Google Cloud Storage (GCS)** bucket, while continuously monitoring **system performance** and **network metrics**.  
It dynamically adapts the frame rate to balance throughput and stability.

## Measurements
- Per-frame upload latency and retries
- Aggregate throughput (Mbps)
- Total upload size and duration
- Real FPS vs. target FPS (with adaptive throttling)
- Continuous CPU/RAM usage and network I/O (per NIC or aggregate)

## Prereqs
- Python 3.9+
- Install the Google Cloud CLI (`gcloud`)
- A GCS bucket configured (e.g., `gs://your-bucket`)
- gcloud authentication
- gcloud auth application-default login
- A functional webcam or video feed (accessible with OpenCV)

## Example of use
```bash
python .\constant_upload_test.py --bucket       upload-bucket-ricardo --prefix stream_test/ --outdir ".\outputs\stream_res" --max-seconds 25 --max-fps 300 --init-fps 30

```
## Arguments:
- `--bucket` (**required**) → GCS bucket name (no `gs://`)
- `--prefix` → Destination folder in GCS (default: `stream`)
- `--outdir` → Local results directory (default: `results_stream`)
- `--concurrency` → Parallel upload threads (default: 4)
- `--retries` → Upload retries per frame (default: 3)
- `--timeout-s` → Per-frame upload timeout (seconds, default: 60)
- `--init-fps`, `--min-fps`, `--max-fps` → Frame rate control (default: 5 / 1 / 30)
- `--max-seconds` → Stop after this many seconds (default: 300)
- `--max-mb` → Stop after uploading this many MB (default: 500)
- `--queue-size` → Max frames in upload queue (default: 20)
- `--force-resolution` → Set to 1 to force webcam frames to 720p (default: 0)
- `--nic` → Network interface to monitor (default: aggregate)
- `--sys-interval` → Sampling interval for system stats in seconds (default: 1.0)

## Help
For more help and to see all available arguments:
```bash
python stream_webcam_gcs.py --help
```