"""
Pełny pipeline Broadsign: Bronze → Silver → Gold

Uruchomienie (codziennie z crona):
    python run_pipeline.py

Etapy:
  1. Bronze — fetch wszystkich źródeł API (Direct, Control, popstats)
  2. Silver — join i wzbogacenie tabel analitycznych
  3. Gold   — star schema gotowy do Power BI

Wyjście: Data/gold/*.parquet  (docelowe dla Power BI / SharePoint)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from datetime import datetime

from Pipeline.bronze.run_all import run as run_bronze
from Pipeline.silver.run_all import run as run_silver
from Pipeline.gold.run_all   import run as run_gold


def run():
    start = datetime.now()
    print(f"{'='*60}")
    print(f"  Broadsign pipeline  {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*60}")

    print("\n>>> [1/3] Bronze pipeline...")
    bronze_ok = run_bronze()

    print("\n>>> [2/3] Silver pipeline...")
    silver_ok = run_silver()

    print("\n>>> [3/3] Gold pipeline...")
    gold_ok = run_gold()

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*60}")
    print(f"  Łącznie: {elapsed}s")
    print(f"  Bronze: {'OK' if bronze_ok else 'FAIL'}")
    print(f"  Silver: {'OK' if silver_ok else 'FAIL'}")
    print(f"  Gold:   {'OK' if gold_ok else 'FAIL'}")
    print(f"{'='*60}")

    return bronze_ok and silver_ok and gold_ok


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
