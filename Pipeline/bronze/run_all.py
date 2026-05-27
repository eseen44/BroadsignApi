"""
Bronze pipeline — odpala pełny fetch wszystkich tabel.

Użycie:
    "C:/ProgramData/anaconda3/python.exe" Pipeline/bronze/run_all.py

Wyniki lądują w Data/bronze/*.parquet
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime

from Package.auth import get_session as get_direct_session
from Package.control.client import get_session as get_control_session
from Pipeline.bronze.fetch_direct import (
    fetch_proposals,
    fetch_proposal_items,
    fetch_screens,
    fetch_screens_frames_mapping,
    fetch_fill_rate,
    fetch_users,
)
from Pipeline.bronze.fetch_control import fetch_all_control

STEPS = [
    # (label, fn, args)
    ("proposals",              fetch_proposals,              "direct"),
    ("proposal_items",         fetch_proposal_items,         "direct"),
    ("screens",                fetch_screens,                "direct"),
    ("screens_frames_mapping", fetch_screens_frames_mapping, "direct"),
    ("fill_rate",              fetch_fill_rate,              "direct"),
    ("users (Direct)",         fetch_users,                  "direct"),
]


if __name__ == "__main__":
    start = datetime.now()
    print(f"=== Bronze pipeline start: {start:%Y-%m-%d %H:%M:%S} ===\n")

    results = {}

    # --- Direct API ---
    print("--- Direct API ---")
    direct_session = get_direct_session()
    for label, fn, _ in STEPS:
        print(f"\n[{label}]")
        try:
            fn(direct_session)
            results[label] = "OK"
        except Exception as e:
            print(f"  BŁĄD: {e}")
            results[label] = f"FAIL: {e}"

    # --- Control API ---
    print("\n--- Control API ---")
    control_session = get_control_session()
    ctrl_results = fetch_all_control(control_session)
    for name, r in ctrl_results.items():
        results[f"ctrl_{name}"] = "OK" if r["ok"] else f"FAIL: {r.get('error')}"

    # --- Podsumowanie ---
    elapsed = datetime.now() - start
    print(f"\n=== Bronze pipeline koniec ({elapsed.seconds}s) ===")
    for label, status in results.items():
        icon = "OK " if status == "OK" else "ERR"
        print(f"  {icon}  {label}: {status}")
