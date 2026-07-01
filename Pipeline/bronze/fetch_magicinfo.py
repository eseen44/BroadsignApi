"""
Bronze fetch -- MagicInfo Proof of Play (metro Liveline / StroerTV / Triplay)

Format ekranu wyznaczany jest na podstawie rozdzielczosci contentu w CMS:
  1920 x 540  -> liveline
  1920 x 1080 -> stroertv
  3840 x 2160 -> triplay
  inne        -> other

Uruchamianie:
  python -m Pipeline.bronze.fetch_magicinfo
  python -m Pipeline.bronze.fetch_magicinfo --force    # ponownie pobierz znane miesiace

Schemat wyjsciowy (magicinfo_pop.parquet):
  format        -- "liveline" | "stroertv" | "triplay" | "other"
  content_id    -- UUID contentu w MagicInfo
  content_name  -- nazwa pliku/kampanii
  content_res   -- rozdzielczosc z CMS, np. "1920 x 540"
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

CURSORS_KEY = "magicinfo_fetched_months"
OUT_NAME    = "magicinfo_pop"
STATS_PATH  = "restapi/v2.0/ems/statistics/contents"
CMS_PATH    = "restapi/v2.0/cms/contents"
GROUP_IDS   = [1]    # STROER -- wszystkie ekrany metro
BATCH_SIZE  = 100    # contentIds per request

RES_FORMAT = {
    "1920 x 540":  "liveline",
    "1920 x 1080": "stroertv",
    "3840 x 2160": "triplay",
}


# ---------------------------------------------------------------------------
# Content metadata (id -> name, resolution, format)
# ---------------------------------------------------------------------------

def fetch_content_meta(mi: MagicInfoClient) -> dict[str, dict]:
    """Zwraca {content_id: {name, resolution, format}} dla wszystkich contentow w CMS."""
    print("  Pobieranie metadanych contentow z CMS...")
    meta, start = {}, 1
    while True:
        r = mi.get(CMS_PATH, params={"pageSize": 200, "startIndex": start})
        items = _extract_items(r)
        if not items:
            break
        for x in items:
            cid = x.get("contentId")
            if not cid:
                continue
            res = (x.get("resolution") or "").strip()
            meta[cid] = {
                "name":   x.get("contentName", ""),
                "res":    res,
                "format": RES_FORMAT.get(res, "other"),
            }
        total = r.get("totalCount", 0)
        if len(items) < 200 or len(meta) >= total:
            break
        start += 200
    print(f"  -> {len(meta)} contentow (rozdzielczosci: "
          f"{ {v: sum(1 for m in meta.values() if m['format']==v) for v in ['liveline','stroertv','triplay','other']} })")
    return meta


# ---------------------------------------------------------------------------
# Play data per month
# ---------------------------------------------------------------------------

def _month_range(year_month: str) -> tuple[str, str]:
    y, m = int(year_month[:4]), int(year_month[5:7])
    last = calendar.monthrange(y, m)[1]
    return f"{year_month}-01", f"{year_month}-{last:02d}"


def fetch_month(mi: MagicInfoClient, content_meta: dict, year_month: str) -> pd.DataFrame:
    start_date, end_date = _month_range(year_month)
    content_ids = list(content_meta.keys())
    batches = [content_ids[i:i + BATCH_SIZE] for i in range(0, len(content_ids), BATCH_SIZE)]
    print(f"  {year_month}: {len(batches)} batchy x {BATCH_SIZE} contentIds...")

    all_rows = []
    for batch_no, batch in enumerate(batches, 1):
        body = {
            "data":       "playFrequency",
            "format":     "frequencyTable",
            "contentIds": batch,
            "groupIds":   GROUP_IDS,
            "time":       "custom",
            "unit":       "day",
            "startDate":  start_date,
            "endDate":    end_date,
        }
        last_exc = None
        for attempt in range(1, 4):
            try:
                r = mi.post(STATS_PATH, body=body, timeout=120)
                for item in _extract_items(r):
                    cid  = item.get("contentId", "")
                    meta = content_meta.get(cid, {})
                    all_rows.append({
                        "format":        meta.get("format", "other"),
                        "content_id":    cid,
                        "content_name":  item.get("contentName", ""),
                        "content_res":   meta.get("res", ""),
                        "play_date":     item.get("timeString", ""),
                        "play_count":    int(item.get("playCount") or 0),
                        "duration_secs": int(item.get("duration") or 0),
                    })
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                resp = getattr(e, "response", None)
                print(f"    [WARN] batch {batch_no} proba {attempt}: "
                      f"{getattr(resp,'status_code','timeout')} {str(e)[:80]}")
        if last_exc is not None:
            print(f"    [ERROR] batch {batch_no} pominiety po 3 probach")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["play_date"] = pd.to_datetime(df["play_date"], errors="coerce").dt.date.astype(str)
    df = df.drop_duplicates(subset=["format", "content_id", "play_date"])

    summary = df.groupby("format")["play_count"].agg(["count","sum"])
    for fmt, row in summary.iterrows():
        print(f"    {fmt}: {int(row['count'])} wierszy, {int(row['sum']):,} emisji")
    return df


# ---------------------------------------------------------------------------
# Parquet upsert
# ---------------------------------------------------------------------------

def _upsert_month(df: pd.DataFrame, year_month: str) -> None:
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
    print(f"  -> {year_month}: {len(df)} wierszy -> lacznie {len(merged)} w {path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(force: bool = False) -> None:
    print("\n=== MagicInfo Bronze Fetch ===")

    fetched = set(load_cursors().get(CURSORS_KEY, []))
    if fetched:
        print(f"  Juz pobrane: {sorted(fetched)}")

    with MagicInfoClient() as mi:

        print("\n[1] Dostepne miesiace PoP...")
        try:
            r = mi.post(STATS_PATH, body={"data": "popFileHistoryPeriods"})
            available = sorted(p["tableDate"] for p in _extract_items(r) if p.get("tableDate"))
            print(f"  Dostepne: {available}")
        except Exception as e:
            print(f"  BLAD: {e}")
            return

        to_fetch = [m for m in available if m not in fetched or force]
        if not to_fetch:
            print("  Wszystkie miesiace pobrane. Uzyj --force zeby odswiezic.")
            return
        print(f"  Do pobrania: {to_fetch}")

        print("\n[2] Metadane contentow z CMS...")
        content_meta = fetch_content_meta(mi)
        if not content_meta:
            print("  BLAD: brak contentow")
            return

        for year_month in to_fetch:
            print(f"\n[3] {year_month}")
            df = fetch_month(mi, content_meta, year_month)
            if len(df) > 0:
                _upsert_month(df, year_month)
                fetched.add(year_month)
                save_cursor(CURSORS_KEY, sorted(fetched))
            else:
                print(f"  Brak danych dla {year_month}")

    print("\n=== Koniec ===")


if __name__ == "__main__":
    main(force="--force" in sys.argv)
