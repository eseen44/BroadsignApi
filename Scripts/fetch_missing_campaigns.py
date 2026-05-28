"""
Jednorazowy fetch kampanii których brakuje w bronze proposals/proposal_items.

Kampanie znane z ctrl_reservations_v22 ale nieobecne w Direct API bronze:
  2654184 — Roadside_Promo
  2539977 — Rossmann copy
  2984174 — Promo Road mk2

Uruchomienie:
  "C:/ProgramData/anaconda3/python.exe" Scripts/fetch_missing_campaigns.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from Package.auth import get_session
from Pipeline.bronze.utils import BRONZE_DIR, upsert_parquet

MISSING_CAMPAIGN_IDS = [2654184, 2539977, 2984174]

PROPOSALS_URL     = "https://direct.broadsign.com/api/v1/proposal/search"
PROP_ITEMS_URL    = "https://direct.broadsign.com/api/v1/proposal_item/search"


def fetch_proposals_by_ids(session, ids):
    """Pobiera konkretne proposal-e po ID (po jednym — API nie ma filtra list)."""
    results = []
    for cid in ids:
        resp = session.post(PROPOSALS_URL, json={
            "$skip": 0, "$top": 10,
            "$sort": [{"field": "id", "dir": "desc"}],
            "id": cid,
        })
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if data:
            results.extend(data)
            print(f"  proposal {cid}: OK — {data[0].get('name', '?')[:60]}")
        else:
            print(f"  proposal {cid}: BRAK w Direct API")
    return results


def fetch_items_by_campaign_ids(session, campaign_ids):
    """Pobiera proposal_items dla podanych campaign_id."""
    results = []
    for cid in campaign_ids:
        resp = session.post(PROP_ITEMS_URL, json={
            "$skip": 0, "$top": 500,
            "campaign_id": cid,
        })
        resp.raise_for_status()
        data = resp.json()
        items = data.get("proposal_items", [])
        results.extend(items)
        print(f"  items dla campaign {cid}: {len(items)} lineitems")
    return results


def run():
    print("Logowanie...")
    session = get_session()

    print("\n[1] Pobieranie proposals...")
    proposals = fetch_proposals_by_ids(session, MISSING_CAMPAIGN_IDS)

    if proposals:
        df_prop = pd.DataFrame(proposals)
        print(f"  Lacznie: {len(df_prop)} proposals")
        upsert_parquet(df_prop, "proposals", key_col="id")
        print("  -> upsert do proposals.parquet: OK")
    else:
        print("  Zadne proposals nie znalezione w Direct API")

    print("\n[2] Pobieranie proposal_items...")
    # Probuj po campaign_id ktore faktycznie trafily
    found_ids = [int(p["id"]) for p in proposals] if proposals else MISSING_CAMPAIGN_IDS
    items = fetch_items_by_campaign_ids(session, found_ids)

    if items:
        df_items = pd.DataFrame(items)
        print(f"  Lacznie: {len(df_items)} lineitems")
        upsert_parquet(df_items, "proposal_items", key_col="id")
        print("  -> upsert do proposal_items.parquet: OK")
    else:
        print("  Brak lineitems")

    print("\nGotowe. Uruchom teraz:")
    print("  python Pipeline/silver/run_all.py")
    print("  python Pipeline/gold/run_all.py")


if __name__ == "__main__":
    run()
