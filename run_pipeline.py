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


# Warstwy leca po kolei i KAZDA jest bramka dla nastepnej -- FAIL przerywa
# pipeline zamiast liczyc dalej na niekompletnych danych.
#
# Dlaczego to wazne: gold/run_all.py na koncu kopiuje Data/gold/*.parquet do
# /dane/OneDrive/Pulpit/UbuntuSynch (a cron */15 wypycha to na SharePoint do
# Power BI). Gold pilnuje tylko WLASNYCH krokow -- jesli wszystkie przejda,
# kopiuje, nie majac pojecia ze bronze padl i policzyl sie ze starych danych.
# Bez tej bramki reczne `python run_pipeline.py` na VM moglo wypchnac do PBI
# gold policzony z nieodswiezonego bronze'a.
#
# `run_bronze/silver/gold` zwracaja False tylko gdy padl krok KRYTYCZNY --
# kroki z NON_CRITICAL (np. magicinfo_pop) nie zatrzymuja pipeline'u.
LAYERS = [
    ("Bronze", run_bronze),
    ("Silver", run_silver),
    ("Gold",   run_gold),
]


def run():
    start = datetime.now()
    print(f"{'='*60}")
    print(f"  Broadsign pipeline  {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*60}")

    status = {}
    for i, (name, fn) in enumerate(LAYERS, start=1):
        print(f"\n>>> [{i}/{len(LAYERS)}] {name} pipeline...")
        ok = fn()
        status[name] = "OK" if ok else "FAIL"

        if not ok:
            skipped = [n for n, _ in LAYERS[i:]]
            for n in skipped:
                status[n] = "POMINIETE"
            print(f"\n!!! {name} padl na krytycznym kroku -- PRZERYWAM pipeline.")
            if skipped:
                print(f"!!! Nie uruchamiam: {', '.join(skipped)}.")
                print("!!! Dane w Data/ i na SharePoint zostaja z poprzedniego przebiegu")
                print("!!! (lepiej stare-ale-spojne niz swiezo policzone z dziury).")
            break

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*60}")
    print(f"  Łącznie: {elapsed}s")
    for name, _ in LAYERS:
        print(f"  {name+':':8s}{status[name]}")
    print(f"{'='*60}")

    return all(status[name] == "OK" for name, _ in LAYERS)


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
