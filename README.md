# GCS Upload Performance Test

This small toolkit lets you measure upload performance to **Google Cloud Storage** (GCS) from your machine while you switch between **Wi-Fi** and **5G**.

## Measurements
- Per-file latency and retries
- Total wall-clock time
- Aggregate throughput (Mbps)
- Error rate
- Continuous CPU/RAM and network I/O while uploading

---

## Prereqs
- Python 3.9+
- Install the Google Cloud CLI (`gcloud`)
- A GCS bucket configured (e.g., `gs://bucket`)
- gcloud auth application-default login

## Example of use:
python .\cloud_perf_test.py 
  --folder ".\TEST DATA\test_folder" 
  --bucket upload-bucket-ricardo 
  --prefix test_data/ 
  --outdir ".\outputs\test_run"

## HELP
To see available arguments and get help, run the script with the --help flag in the terminal:
python .\cloud_perf_test.py --help


