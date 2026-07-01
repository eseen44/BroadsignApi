"""
Bronze fetch -- MagicInfo Proof of Play (metro Liveline/StroerTV)

Zastepuje sztuczne play_logs Broadsign dla ekranow metro realnymi
danymi odtworzeniowymi z Samsung MagicInfo.

Uruchamianie:
  python -m Pipeline.bronze.fetch_magicinfo
  python -m Pipeline.bronze.fetch_magicinfo --force    # ponownie pobierz juz znane miesiace

Schemat wyjsciowy (magicinfo_pop.parquet):
  content_id    -- UUID contentu w MagicInfo
  content_name  -- nazwa pliku/kampanii
  play_date     -- data (YYYY-MM-DD)
  play_count    -- liczba odtworzen
  duration_secs -- laczny czas odtworzenia w sekundach
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import calendar
from datetime import datetime, timezone

import pandas as pd

from Package.magicinfo.client import MagicInfoClient, _extract_items
from Pipeline.bronze.utils import BRONZE_DIR, load_cursors, save_cursor

CURSORS_KEY = "magicinfo_fetched_months"   # list of "YYYY-MM" strings
OUT_NAME    = "magicinfo_pop"
STATS_PATH  = "restapi/v2.0/ems/statistics/contents"
CMS_PATH    = "restapi/v2.0/cms/contents"
GROUP_IDS   = [1]          # 1 = STROER (wszystkie 4130 ekranow metro)
BATCH_SIZE  = 500          # contentIds na jedno wywolanie API


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _month_range(year_month: str) -> tuple[str, str]:
    """'2026-06' -> ('2026-06-01', '2026-06-30')"""
    y, m = int(year_month[:4]), int(year_month[5:7])
    last = calendar.monthrange(y, m)[1]
    return f"{year_month}-01", f"{year_month}-{last:02d}"


def _upsert_month(df: pd.DataFrame, year_month: str) -> None:
    """Zastepuje wszystkie wiersze dla danego miesiaca w parquecie."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    path = BRONZE_DIR / f"{OUT_NAME}.parquet"
    df = df.copy()
    df["_fetched_at"] = datetime.now(timezone.utc).isoformat()

    if path.exists():
        existing = pd.read_parquet(path)
        keep = existing[~existing["play_date"].astype(str).str.startswith(year_month)]
        merged = pd.concat([keep, df], ignore_index=True)
    else:
        merged = df

    merged.to_parquet(path, index=False, engine="pyarrow")
    print(f"  -> [upsert_month] {year_month}: {len(df)} wierszy -> lacznie {len(merged)} w {path.name}")


# ---------------------------------------------------------------------------
# Fetch content IDs
# ---------------------------------------------------------------------------

def fetch_all_content_ids(mi: MagicInfoClient) -> list[str]:
    """Zwraca liste wszystkich content UUID z MagicInfo CMS."""
    print("  Pobieranie listy contentow z CMS...")
    ids = []
    start = 1
    page_size = 200
    while True:
        r = mi.get(CMS_PATH, params={"pageSize": page_size, "startIndex": start})
        items = _extract_items(r)
        if not items:
            break
        ids.extend(x["contentId"] for x in items if x.get("contentId"))
        total = r.get("totalCount", 0)
        print(f"    startIndex={start}: {len(items)} items ({len(ids)}/{total} lacznie)")
        if len(items) < page_size or len(ids) >= total:
            break
        start += page_size
    print(f"  -> Pobrano {len(ids)} content IDs")
    return ids


# ---------------------------------------------------------------------------
# Fetch play data for one month
# ---------------------------------------------------------------------------

def fetch_month(mi: MagicInfoClient, content_ids: list[str], year_month: str) -> pd.DataFrame:
    """
    Pobiera playFrequency dla calego miesiaca.
    Wywoluje API w porcjach BATCH_SIZE contentIds aby nie przekroczyc limitu.
    Zwraca DataFrame ze schematem: content_id, content_name, play_date, play_count, duration_secs.
    """
    start_date, end_date = _month_range(year_month)
    print(f"  Pobieranie {year_month} ({start_date} do {end_date})")

    all_rows = []
    batches = [content_ids[i:i + BATCH_SIZE] for i in range(0, len(content_ids), BATCH_SIZE)]

    for batch_no, batch in enumerate(batches, 1):
        print(f"    batch {batch_no}/{len(batches)} ({len(batch)} contentIds)")
        try:
            r = mi.post(STATS_PATH, body={
                "data":      "playFrequency",
                "format":    "frequencyTable",
                "contentIds": batch,
                "groupIds":  GROUP_IDS,
                "time":      "custom",
                "unit":      "day",
                "startDate": start_date,
                "endDate":   end_date,
            })
            items = _extract_items(r)
            for item in items:
                all_rows.append({
                    "content_id":    item.get("contentId", ""),
                    "content_name":  item.get("contentName", ""),
                    "play_date":     item.get("timeString", ""),
                    "play_count":    int(item.get("playCount") or 0),
                    "duration_secs": int(item.get("duration") or 0),
                })
        except Exception as e:
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", "?")
            body = (getattr(resp, "text", "") or "")[:300]
            print(f"    [WARN] batch {batch_no} -> {status}: {body}")

    if not all_rows:
        print(f"  -> Brak danych dla {year_month}")
        return pd.DataFrame(columns=["content_id", "content_name", "play_date", "play_count", "duration_secs"])

    df = pd.DataFrame(all_rows)
    df["play_date"] = pd.to_datetime(df["play_date"], errors="coerce").dt.date.astype(str)
    # Deduplikuj (ten sam content moze sie pojawic w roznych batchach jesli ma rozne ID)
    df = df.drop_duplicates(subset=["content_id", "play_date"])
    print(f"  -> {len(df)} wierszy dla {year_month} ({df['content_name'].nunique()} unikalnych contentow)")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(force: bool = False) -> None:
    print("\n=== MagicInfo Bronze Fetch ===")

    fetched = set(load_cursors().get(CURSORS_KEY, []))
    if fetched:
        print(f"  Juz pobrane miesiace: {sorted(fetched)}")

    with MagicInfoClient() as mi:

        # 1. Dostepne miesiace z API
        print("\n[1] Pobieranie dostepnych miesiecy PoP...")
        try:
            r = mi.post(STATS_PATH, body={"data": "popFileHistoryPeriods"})
            periods = _extract_items(r)
            available = sorted(p["tableDate"] for p in periods if p.get("tableDate"))
            print(f"  Dostepne: {available}")
        except Exception as e:
            print(f"  BLAD pobierania miesiecy: {e}")
            return

        to_fetch = [m for m in available if m not in fetched or force]
        if not to_fetch:
            print("  Wszystkie miesiace juz pobrane. Uzyj --force zeby odswiezic.")
            return
        print(f"  Do pobrania: {to_fetch}")

        # 2. Pobierz wszystkie content IDs raz
        print("\n[2] Pobieranie content IDs z CMS...")
        content_ids = fetch_all_content_ids(mi)
        if not content_ids:
            print("  BLAD: brak content IDs")
            return

        # 3. Pobierz playFrequency per miesiac
        for year_month in to_fetch:
            print(f"\n[3] Miesiac: {year_month}")
            df = fetch_month(mi, content_ids, year_month)
            if len(df) > 0:
                _upsert_month(df, year_month)
                fetched.add(year_month)
                save_cursor(CURSORS_KEY, sorted(fetched))
            else:
                print(f"  Pomijam zapis dla {year_month} (brak danych)")

    print("\n=== Koniec ===")


if __name__ == "__main__":
    force = "--force" in sys.argv
    main(force=force)
