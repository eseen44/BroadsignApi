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
    reconcile_missing_proposal_items,
    fetch_screens,
    fetch_screens_frames_mapping,
    fetch_fill_rate,
    fetch_users,
)
from Pipeline.bronze.fetch_control import fetch_all_control, fetch_reservations_v22
from Pipeline.bronze.fetch_play_logs import import_historical, fetch_incremental, fetch_resources
from Pipeline.bronze.fetch_magicinfo import main as fetch_magicinfo

# Kroki, ktorych FAIL nie blokuje reszty pipeline'u (Silver/Gold nie zaleza
# jeszcze od ich wyniku).
NON_CRITICAL = {"magicinfo_pop", "bronze_backup_s3"}


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

    simple_steps = [
        ("proposals",              fetch_proposals),
        ("proposal_items",         fetch_proposal_items),
        ("reconcile_proposal_items", reconcile_missing_proposal_items),
        ("screens",                fetch_screens),
        ("screens_frames_mapping", fetch_screens_frames_mapping),
        ("users",                  fetch_users),
    ]
    for label, fn in simple_steps:
        print(f"\n[{label}]")
        try:
            fn(direct)
            results[label] = "OK"
        except Exception as e:
            print(f"  BLAD: {e}")
            results[label] = f"FAIL: {e}"

    # fill_rate — screen_ids bierzemy z bronze zamiast wołać API drugi raz
    print("\n[fill_rate]")
    try:
        import pandas as pd
        from Pipeline.bronze.utils import BRONZE_DIR
        screens_parquet = BRONZE_DIR / "screens.parquet"
        if screens_parquet.exists():
            screen_ids = pd.read_parquet(screens_parquet)["id"].tolist()
        else:
            screen_ids = None  # fallback: fetch_fill_rate pobierze sam
        fetch_fill_rate(direct, screen_ids=screen_ids)
        results["fill_rate"] = "OK"
    except Exception as e:
        print(f"  BLAD: {e}")
        results["fill_rate"] = f"FAIL: {e}"

    # ------------------------------------------------------------------
    # 2. Control API — overwrite / incremental upsert
    # ------------------------------------------------------------------
    print("\n--- Control API (overwrite + incremental) ---")
    control = get_control_session()
    ctrl_results = fetch_all_control(control)
    for name, r in ctrl_results.items():
        results[f"ctrl_{name}"] = "OK" if r["ok"] else f"FAIL: {r.get('error')}"

    print("\n[ctrl_reservations_v22 — proposal_line_item_id]")
    try:
        r = fetch_reservations_v22(control)
        results["ctrl_reservations_v22"] = f"OK ({r['rows']} wierszy)" if r["ok"] else f"FAIL"
    except Exception as e:
        print(f"  BLAD: {e}")
        results["ctrl_reservations_v22"] = f"FAIL: {e}"

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

    popstats = get_popstats_session()

    print("\n[play_logs / incremental popstats]")
    try:
        pr = fetch_incremental(popstats)
        results["play_logs_incremental"] = f"OK (+{pr['new_files']} pliki, +{pr['new_rows']} wierszy)"
    except Exception as e:
        print(f"  BLAD: {e}")
        results["play_logs_incremental"] = f"FAIL: {e}"

    print("\n[resources_latest]")
    try:
        n = fetch_resources(popstats)
        results["resources_latest"] = f"OK ({n} zasobow)"
    except Exception as e:
        print(f"  BLAD: {e}")
        results["resources_latest"] = f"FAIL: {e}"

    # ------------------------------------------------------------------
    # 4. MagicInfo — PoP metro (liveline/stroertv/triplay), niekrytyczne
    # ------------------------------------------------------------------
    print("\n--- MagicInfo (metro PoP) ---")
    print("\n[magicinfo_pop]")
    try:
        fetch_magicinfo()
        results["magicinfo_pop"] = "OK"
    except Exception as e:
        print(f"  BLAD: {e}")
        results["magicinfo_pop"] = f"FAIL: {e}"

    # ------------------------------------------------------------------
    # 5. Backup bronze na S3 -- niekrytyczne. Bronze zawiera dane, ktorych
    #    NIE DA SIE odtworzyc (popstats rolling ~2mc, MagicInfo ~31 dni),
    #    a /dane na VM nie jest przez nic innego backupowane. Non-critical,
    #    zeby chwilowa awaria S3 (VPN, throttling) nie wywalala pipeline'u,
    #    ktory bronze policzyl poprawnie.
    # ------------------------------------------------------------------
    print("\n[bronze_backup_s3]")
    try:
        from Pipeline.bronze.backup_to_s3 import backup
        wyniki = backup()
        zle = [k for k, v in wyniki.items() if not v.startswith("OK")]
        if zle:
            results["bronze_backup_s3"] = f"FAIL: {zle}"
        else:
            results["bronze_backup_s3"] = f"OK ({len(wyniki)} plikow)"
    except Exception as e:
        print(f"  BLAD: {e}")
        results["bronze_backup_s3"] = f"FAIL: {e}"

    # ------------------------------------------------------------------
    # Podsumowanie
    # ------------------------------------------------------------------
    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*55}")
    print(f"  Koniec ({elapsed}s)")
    print(f"{'='*55}")
    ok   = [k for k, v in results.items() if v.startswith("OK")]
    fail = [k for k, v in results.items() if not v.startswith("OK")]
    critical_fail = [k for k in fail if k not in NON_CRITICAL]
    for k in ok:
        print(f"  OK   {k}: {results[k]}")
    for k in fail:
        tag = "FAIL (non-critical)" if k in NON_CRITICAL else "FAIL"
        print(f"  {tag} {k}: {results[k]}")

    return len(critical_fail) == 0


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
