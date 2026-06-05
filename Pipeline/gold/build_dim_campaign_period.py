"""
Gold — dim_campaign_period

Bridge table: campaign_id x year_month

Jeden wiersz dla każdego miesiąca w którym kampania była aktywna
(wg dat line itemów). Służy jako "selection dimension" w Power BI —
slicer po year_month filtruje KTÓRE kampanie pokazać, bez filtrowania
dat faktycznych emisji.

Grain: campaign_id x year_month (unikalne pary)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import save_gold

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"


def build_dim_campaign_period():

    ACTIVE_STATUSES = {"live", "ended", "submitted", "booked"}

    dli = pd.read_parquet(
        GOLD_DIR / "dim_line_item.parquet",
        columns=["campaign_id", "line_start", "line_end", "status_name"],
    )
    dli["campaign_id"] = pd.to_numeric(dli["campaign_id"], errors="coerce")
    dli["line_start"]  = pd.to_datetime(dli["line_start"], errors="coerce")
    dli["line_end"]    = pd.to_datetime(dli["line_end"],   errors="coerce")
    dli = dli.dropna(subset=["campaign_id", "line_start", "line_end"])

    # Tylko aktywne statusy — pomijamy cancelled, draft itp.
    before = len(dli)
    dli = dli[dli["status_name"].str.lower().isin(ACTIVE_STATUSES)]
    print(f"  Odfiltrowano {before - len(dli)} LI (cancelled/draft), zostaje {len(dli)}")

    # Iterujemy per LINE ITEM (nie per kampania) żeby nie rozciągać zakresu
    # między niepowiązanymi line itemami tej samej kampanii.
    # Potem deduplikujemy po (campaign_id, year_month).
    rows = []
    for _, r in dli.iterrows():
        months = pd.period_range(
            start=r["line_start"].to_period("M"),
            end=r["line_end"].to_period("M"),
            freq="M",
        )
        for m in months:
            rows.append({
                "campaign_id": int(r["campaign_id"]),
                "year_month":  str(m),
                "year_month_dt": m.to_timestamp(),
            })

    df = pd.DataFrame(rows).drop_duplicates(subset=["campaign_id", "year_month"])
    df["campaign_id"] = df["campaign_id"].astype("int64")
    df = df.sort_values(["year_month", "campaign_id"])

    print(f"  Wierszy: {len(df):,}")
    print(f"  Unikalne miesiące: {df['year_month'].nunique()}")
    print(f"  Unikalne kampanie: {df['campaign_id'].nunique()}")

    save_gold(df, "dim_campaign_period")


if __name__ == "__main__":
    build_dim_campaign_period()
