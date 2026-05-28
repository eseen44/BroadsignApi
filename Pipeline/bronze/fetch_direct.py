"""
Bronze fetch — Direct API (direct.broadsign.com)

Tabele:
  proposals              — kampanie (booking layer)
  proposal_items         — line itemy
  screens                — ekrany digital (inventory view)
  screens_frames_mapping — bridge screen <-> frame/group
  fill_rate              — fill rate breakdown (ostatnie N dni)
  users                  — użytkownicy / handlowcy
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from datetime import date, timedelta

from Package.auth import get_session
from Package.direct.proposals import get_all_proposals
from Package.direct.proposal_items import get_all_proposal_items
from Package.direct.inventory import get_all_screens, get_screens_frames_mapping
from Package.direct.reporting import get_all_fill_rate
from Pipeline.bronze.utils import save_parquet, upsert_parquet

FILL_RATE_DAYS = 28  # ile dni wstecz dla fill_rate — API max: 1 miesiąc


def fetch_proposals(session):
    print("Proposals...")
    records = get_all_proposals(session)
    df = pd.DataFrame(records)
    return upsert_parquet(df, "proposals", key_col="id")


def fetch_proposal_items(session):
    print("Proposal items...")
    records = get_all_proposal_items(session)
    df = pd.DataFrame(records)
    return upsert_parquet(df, "proposal_items", key_col="id")


def fetch_screens(session):
    print("Screens...")
    records = get_all_screens(session, inventory_type="digital")
    df = pd.DataFrame(records)
    return save_parquet(df, "screens")


def fetch_screens_frames_mapping(session):
    print("Screens-frames mapping...")
    records = get_screens_frames_mapping(session)
    df = pd.DataFrame(records)
    before = len(df)
    # API czasem zwraca identyczne duplikaty — usuwamy
    df = df.drop_duplicates(subset=["frame_id"])
    if len(df) < before:
        print(f"  Usunieto {before - len(df)} zduplikowanych wierszy (frame_id)")
    return save_parquet(df, "screens_frames_mapping")


def fetch_fill_rate(session, screen_ids=None, days_back=FILL_RATE_DAYS):
    """
    screen_ids: lista ID ekranów (opcjonalnie — jeśli None, pobieramy ze screens bronze).
    Przekazanie screen_ids z wcześniej pobranego fetch_screens() oszczędza jedno API call.
    """
    print(f"Fill rate (ostatnie {days_back} dni)...")
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back - 1)
    print(f"  Zakres: {start_date} -> {end_date}")

    if screen_ids is None:
        screens = get_all_screens(session, inventory_type="digital")
        screen_ids = [s["id"] for s in screens]

    records = get_all_fill_rate(session, screen_ids, start_date, end_date)
    if not records:
        print("  Brak danych fill rate.")
        return None
    df = pd.DataFrame(records)
    return save_parquet(df, "fill_rate")


def fetch_users(session):
    print("Users...")
    PAGE_SIZE = 100
    results = []
    skip = 0
    while True:
        resp = session.post(
            "https://direct.broadsign.com/api/v1/user/search",
            json={"$skip": skip, "$top": PAGE_SIZE},
        )
        resp.raise_for_status()
        data = resp.json()
        # Direct API user/search zwraca d.__count / d.results
        batch = data.get("d", {}).get("results", [])
        results.extend(batch)
        total = data.get("d", {}).get("__count", len(results))
        print(f"  {min(skip + PAGE_SIZE, int(total))}/{total}", flush=True)
        skip += PAGE_SIZE
        if skip >= int(total):
            break
    df = pd.DataFrame(results)
    return save_parquet(df, "users")


if __name__ == "__main__":
    session = get_session()
    fetch_proposals(session)
    fetch_proposal_items(session)
    fetch_screens(session)
    fetch_screens_frames_mapping(session)
    fetch_fill_rate(session)
    fetch_users(session)
    print("\nDirect bronze — gotowe.")
