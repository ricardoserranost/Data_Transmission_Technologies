import csv
import json
import argparse
from pathlib import Path

def main():
    # --- CONFIG ---
    ap = argparse.ArgumentParser(description="Get extra metrics from saved results")
    ap.add_argument("--folder", required=True, help="Folder where results were saved")
    ap.add_argument("--outdir", required=False, default=False)
    args = ap.parse_args()

    folder = Path(args.folder)# carpeta donde están sys_metrics.csv y summary.json
    outdir = Path(args.outdir) if args.outdir else None  

    sys_metrics_file = folder / "sys_metrics.csv"
    summary_file = folder / "summary.json"

    # --- 1. Leer bytes_total de summary.json ---
    with open(summary_file) as f:
        summary = json.load(f)
    bytes_total = summary.get("bytes_total", 0)

    # --- 2. Leer bytes_sent inicial y final de sys_metrics.csv ---
    with open(sys_metrics_file) as f:
        reader = csv.DictReader(f)
        bytes_sent_list = [int(row["bytes_sent"]) for row in reader]

    if not bytes_sent_list:
        raise ValueError("No se encontraron datos en sys_metrics.csv")

    delta_bytes_sent = bytes_sent_list[-1] - bytes_sent_list[0]
    print("Last bytes received counter: ", bytes_sent_list[-1])
    print("First bytes received counter: ", bytes_sent_list[0])

    # --- 3. Calcular overhead ---
    overhead_pct = ((delta_bytes_sent - bytes_total) / bytes_total) * 100 if bytes_total > 0 else None

    # --- 4. Mostrar resultados ---
    print(f"Bytes totales subidos (archivos): {bytes_total:,} B")
    print(f"Bytes enviados según NIC: {delta_bytes_sent:,} B")
    print(f"Overhead de red aproximado: {overhead_pct:.2f} %")

    if(outdir):
        metrics = {
            "Bytes totales": bytes_total,
            "Bytes enviados según NIC": delta_bytes_sent,
            "Overhead de red aproximado": overhead_pct
        }

        with open(outdir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)


if __name__ == "__main__":
    main()