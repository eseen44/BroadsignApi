"""
Bronze pipeline — orchestrator.

Uruchomienie:
    python Pipeline/bronze/run_all.py

Strategie odświeżania:
  Direct API  → pełny overwrite (proposals, items, screens, users, fill_rate)
  Control API → overwrite dla małych tabel, incremental upsert dla dużych
  Play logi   → append nowych dni (popstats) + jednorazowy import historyczny

Wyniki: Data/bronze/*.parquet
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime

from Package.auth import get_session as get_direct_session
from Package.control.client import get_session as get_control_session
from Package.popstats.client import get_session as get_popstats_session

from Pipeline.bronze.fetch_direct import (
    fetch_proposals,
    fetch_proposal_items,
    fetch_screens,
    fetch_screens_frames_mapping,
    fetch_fill_rate,
    fetch_users,
)
from Pipeline.bronze.fetch_control import fetch_all_control
from Pipeline.bronze.fetch_play_logs import import_historical, fetch_incremental


def run():
    start = datetime.now()
    print(f"{'='*55}")
    print(f"  Bronze pipeline  {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*55}\n")

    results = {}

    # ------------------------------------------------------------------
    # 1. Direct API — pełny overwrite
    # ------------------------------------------------------------------
    print("--- Direct API (overwrite) ---")
    direct = get_direct_session()

    direct_steps = [
        ("proposals",              fetch_proposals),
        ("proposal_items",         fetch_proposal_items),
        ("screens",                fetch_screens),
        ("screens_frames_mapping", fetch_screens_frames_mapping),
        ("fill_rate",              fetch_fill_rate),
        ("users",                  fetch_users),
    ]
    for label, fn in direct_steps:
        print(f"\n[{label}]")
        try:
            fn(direct)
            results[label] = "OK"
        except Exception as e:
            print(f"  BŁĄD: {e}")
            results[label] = f"FAIL: {e}"

    # ------------------------------------------------------------------
    # 2. Control API — overwrite / incremental upsert
    # ------------------------------------------------------------------
    print("\n--- Control API (overwrite + incremental) ---")
    control = get_control_session()
    ctrl_results = fetch_all_control(control)
    for name, r in ctrl_results.items():
        results[f"ctrl_{name}"] = "OK" if r["ok"] else f"FAIL: {r.get('error')}"

    # ------------------------------------------------------------------
    # 3. Play logi — import historyczny + incremental z popstats
    # ------------------------------------------------------------------
    print("\n--- Play logi (append) ---")
    print("\n[play_logs / historical]")
    try:
        import_historical()
        results["play_logs_historical"] = "OK"
    except Exception as e:
        print(f"  BŁĄD: {e}")
        results["play_logs_historical"] = f"FAIL: {e}"

    print("\n[play_logs / incremental popstats]")
    try:
        popstats = get_popstats_session()
        pr = fetch_incremental(popstats)
        results["play_logs_incremental"] = f"OK (+{pr['new_files']} pliki, +{pr['new_rows']} wierszy)"
    except Exception as e:
        print(f"  BŁĄD: {e}")
        results["play_logs_incremental"] = f"FAIL: {e}"

    # ------------------------------------------------------------------
    # Podsumowanie
    # ------------------------------------------------------------------
    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*55}")
    print(f"  Koniec ({elapsed}s)")
    print(f"{'='*55}")
    ok = [k for k, v in results.items() if v.startswith("OK")]
    fail = [k for k, v in results.items() if not v.startswith("OK")]
    for k in ok:
        print(f"  OK   {k}: {results[k]}")
    for k in fail:
        print(f"  FAIL {k}: {results[k]}")

    return len(fail) == 0


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
