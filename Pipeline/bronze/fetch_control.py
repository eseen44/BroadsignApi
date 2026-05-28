"""
Bronze fetch — Control API (api.broadsign.com:10889)

Dwie strategie odświeżania:
  OVERWRITE  — pełny fetch za każdym razem (małe, rzadko zmienne tabele)
  INCREMENTAL — fetch tylko zmian od ostatniego runu (cursor: not_modified_since)

Potwierdzone wersje endpointów: przetestowane 2026-05-27.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Package.control.client import get_session, fetch_resource
from Pipeline.bronze.utils import save_parquet, upsert_parquet, get_cursor, save_cursor

# Endpointy z ich wersjami API
ENDPOINTS = {
    "reservations":   ("reservation",         7),
    "display_units":  ("display_unit",        5),
    "players":        ("client_registration", 5),
    "content":        ("content",             7),
    "bundles":        ("bundle",             12),
    "bundle_content": ("bundle_content",      3),
    "schedules":      ("schedule",            5),
    "customers":      ("customer",            5),
    "users":          ("user",                6),
}

# Te tabele mają incremental fetch (upsert po id + cursor)
# Reszta → pełny overwrite
INCREMENTAL = {"reservations", "content", "bundles", "bundle_content", "schedules"}


def fetch_reservations_v22(session) -> dict:
    """
    Pobiera pełne dane rezervacji z v22 (po ID) — zawiera proposal_line_item_id
    którego brakuje w standardowym incremental fetchu.
    Zapisuje jako ctrl_reservations_v22.parquet (overwrite przy każdym runie).
    """
    from Pipeline.bronze.utils import BRONZE_DIR

    # Pobierz ID z istniejącego ctrl_reservations
    res_path = BRONZE_DIR / "ctrl_reservations.parquet"
    if not res_path.exists():
        print("  ctrl_reservations.parquet nie istnieje — pomiń v22 fetch")
        return {"rows": 0, "ok": True}

    ids = pd.read_parquet(res_path, columns=["id"])["id"].tolist()
    if not ids:
        return {"rows": 0, "ok": True}

    BATCH = 100
    records = []
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i+BATCH]
        id_str = ",".join(str(x) for x in batch)
        r = session.get(
            f"https://api.broadsign.com:10889/rest/reservation/v22/many?ids={id_str}",
            timeout=60,
        )
        r.raise_for_status()
        records.extend(r.json().get("reservation", []))
        print(f"  v22: {min(i+BATCH, len(ids))}/{len(ids)}", end="\r", flush=True)

    print()
    df = pd.DataFrame(records)
    # Zostaw tylko kluczowe kolumny (resztę mamy w ctrl_reservations)
    keep = ["id", "proposal_id", "proposal_line_item_id", "contract_id",
            "container_id", "booking_state", "has_goal", "goal_unit", "goal_amount"]
    df = df[[c for c in keep if c in df.columns]]
    # Zamień puste stringi na None
    for col in ["proposal_id", "proposal_line_item_id", "contract_id"]:
        if col in df.columns:
            df[col] = df[col].replace("", None)

    save_parquet(df, "ctrl_reservations_v22")
    print(f"  ctrl_reservations_v22: {len(df)} wierszy, "
          f"z proposal_line_item_id: {df['proposal_line_item_id'].notna().sum()}")
    return {"rows": len(df), "ok": True}


def fetch_all_control(session) -> dict:
    results = {}

    for table_name, (resource, version) in ENDPOINTS.items():
        parquet_name = f"ctrl_{table_name}"
        print(f"{table_name}...")

        try:
            if table_name in INCREMENTAL:
                # Pobierz cursor z poprzedniego runu
                cursor_key = f"ctrl_{table_name}"
                since = get_cursor(cursor_key)

                records, new_cursor = fetch_resource(session, resource, version, since=since)
                df = pd.DataFrame(records)

                if len(df) == 0:
                    print(f"  Brak zmian od {since}")
                    results[table_name] = {"rows": 0, "ok": True, "strategy": "incremental/no-change"}
                else:
                    upsert_parquet(df, parquet_name)
                    save_cursor(cursor_key, new_cursor)
                    results[table_name] = {"rows": len(df), "ok": True, "strategy": "incremental"}

            else:
                # Pełny overwrite (zawsze since=1970 żeby dostać wszystko)
                records, _ = fetch_resource(session, resource, version, since="1970-01-01T00:00:00.")
                df = pd.DataFrame(records)
                save_parquet(df, parquet_name)
                results[table_name] = {"rows": len(df), "ok": True, "strategy": "overwrite"}

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
            print(f"  OK  ctrl_{name}: {r['rows']} wierszy [{r.get('strategy','')}]")
        else:
            print(f"  FAIL ctrl_{name}: {r['error']}")
