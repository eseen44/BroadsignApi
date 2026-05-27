"""
Bronze fetch — Control API (api.broadsign.com:10889)

Tabele:
  ctrl_reservations       — kampanie (techniczny odpowiednik proposals)
  ctrl_display_units      — ekrany (techniczny widok)
  ctrl_players            — fizyczne playery (hardware)
  ctrl_content            — pliki kreatywne
  ctrl_bundles            — pakiety kreacji
  ctrl_bundle_content     — bridge bundle <-> content
  ctrl_schedules          — reguły harmonogramu
  ctrl_customers          — klienci / advertisers
  ctrl_users              — użytkownicy Control

Prefix "ctrl_" odróżnia tabele Control API od Direct API w warstwie bronze.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Package.control.client import get_session, fetch_resource
from Pipeline.bronze.utils import save_parquet

# Potwierdzone wersje endpointów (przetestowane 2026-05-27)
ENDPOINTS = {
    "reservations":    ("reservation",         7),
    "display_units":   ("display_unit",        5),
    "players":         ("client_registration", 5),
    "content":         ("content",             7),
    "bundles":         ("bundle",             12),
    "bundle_content":  ("bundle_content",      3),
    "schedules":       ("schedule",            5),
    "customers":       ("customer",            5),
    "users":           ("user",                6),
}


def fetch_all_control(session):
    results = {}
    for table_name, (resource, version) in ENDPOINTS.items():
        print(f"{table_name}...")
        try:
            records, cursor = fetch_resource(session, resource, version)
            df = pd.DataFrame(records)
            save_parquet(df, f"ctrl_{table_name}")
            results[table_name] = {"rows": len(df), "cursor": cursor, "ok": True}
        except Exception as e:
            print(f"  BŁĄD: {e}")
            results[table_name] = {"ok": False, "error": str(e)}
    return results


if __name__ == "__main__":
    session = get_session()
    results = fetch_all_control(session)

    print("\n=== Control bronze — podsumowanie ===")
    for name, r in results.items():
        if r["ok"]:
            print(f"  OK  ctrl_{name}: {r['rows']} wierszy")
        else:
            print(f"  FAIL ctrl_{name}: {r['error']}")
