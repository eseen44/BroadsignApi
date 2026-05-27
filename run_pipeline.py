"""
Pełny pipeline Broadsign: Bronze + Silver

Uruchomienie (codziennie z crona):
    python run_pipeline.py

Etapy:
  1. Bronze — fetch wszystkich źródeł API (Direct, Control, popstats)
  2. Silver — join i wzbogacenie tabel analitycznych

Wyjście: Data/silver/*.parquet  (gotowe do Power BI / SharePoint)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from datetime import datetime

from Pipeline.bronze.run_all import run as run_bronze
from Pipeline.silver.run_all import run as run_silver


def run():
    start = datetime.now()
    print(f"{'='*60}")
    print(f"  Broadsign pipeline  {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*60}")

    print("\n>>> [1/2] Bronze pipeline...")
    bronze_ok = run_bronze()

    print("\n>>> [2/2] Silver pipeline...")
    silver_ok = run_silver()

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*60}")
    print(f"  Łącznie: {elapsed}s")
    print(f"  Bronze: {'OK' if bronze_ok else 'FAIL'}")
    print(f"  Silver: {'OK' if silver_ok else 'FAIL'}")
    print(f"{'='*60}")

    return bronze_ok and silver_ok


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
