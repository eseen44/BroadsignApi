"""
Gold — dim_campaign_period

Bridge table: campaign_id x year_month (grain="M") + campaign_id x year_week (grain="W")

Jeden wiersz dla każdego miesiąca/tygodnia w którym kampania była aktywna
(wg dat line itemów). Służy jako "selection dimension" w Power BI —
slicer po year_month/year_week filtruje KTÓRE kampanie pokazać, bez
filtrowania dat faktycznych emisji.

Wiersze M i W są budowane NIEZALEŻNIE z tego samego źródła (line_start/
line_end per line item) — miesiąc NIE jest wyprowadzany z tygodnia (i na
odwrót), żeby uniknąć przypadków brzegowych (np. line item zaczynający się
1-go dnia miesiąca w środku tygodnia, którego poniedziałek wypada w
poprzednim miesiącu — wyprowadzenie month=f(week) fałszywie dopisałoby
kampanię do poprzedniego miesiąca).

Dla wierszy grain="M": year_month/year_month_dt wypełnione, kolumny
tygodniowe puste. Dla grain="W": year_week/year_week_dt/week_parent_month_dt
wypełnione, year_month/year_month_dt puste. `week_parent_month_dt` (miesiąc
poniedziałku danego tygodnia) służy WYŁĄCZNIE do grupowania wizualnego
(które tygodnie pokazać pod którym miesiącem w UI) — nigdy do filtrowania
kampanii, więc nie zanieczyszcza istniejącej logiki miesięcznej.

Grain: campaign_id x year_month (unikalne pary w obrębie grain="M")
       campaign_id x year_week  (unikalne pary w obrębie grain="W")
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import save_gold

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"


def _build_month_rows(dli: pd.DataFrame) -> pd.DataFrame:
    """Logika bez zmian względem oryginalnej wersji (grain miesięczny)."""
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
    df["grain"] = "M"
    df["year_week"] = None
    df["year_week_dt"] = pd.NaT
    df["week_parent_month_dt"] = pd.NaT
    return df


def _build_week_rows(dli: pd.DataFrame) -> pd.DataFrame:
    """Niezależna generacja tygodniowa (Poniedziałek-Niedziela) z tego
    samego line_start/line_end. Nie zależy od _build_month_rows."""
    rows = []
    for _, r in dli.iterrows():
        first_monday = r["line_start"] - pd.Timedelta(days=r["line_start"].weekday())
        last_monday  = r["line_end"]   - pd.Timedelta(days=r["line_end"].weekday())
        mondays = pd.date_range(first_monday, last_monday, freq="7D")
        for monday in mondays:
            iso_year, iso_week, _ = monday.isocalendar()
            rows.append({
                "campaign_id": int(r["campaign_id"]),
                "year_week": f"{iso_year}-W{iso_week:02d}",
                "year_week_dt": monday.normalize(),
                "week_parent_month_dt": pd.Timestamp(monday.year, monday.month, 1),
            })

    df = pd.DataFrame(rows).drop_duplicates(subset=["campaign_id", "year_week"])
    df["campaign_id"] = df["campaign_id"].astype("int64")
    df["grain"] = "W"
    df["year_month"] = None
    df["year_month_dt"] = pd.NaT
    return df


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
    # Potem deduplikujemy w obrębie każdego grain osobno.
    month_df = _build_month_rows(dli)
    week_df = _build_week_rows(dli)

    cols = ["campaign_id", "grain", "year_month", "year_month_dt",
            "year_week", "year_week_dt", "week_parent_month_dt"]
    df = pd.concat([month_df[cols], week_df[cols]], ignore_index=True)
    df = df.sort_values(["grain", "year_month", "year_week", "campaign_id"])

    print(f"  Wierszy: {len(df):,} (M={len(month_df):,}, W={len(week_df):,})")
    print(f"  Unikalne miesiące: {month_df['year_month'].nunique()}")
    print(f"  Unikalne tygodnie: {week_df['year_week'].nunique()}")
    print(f"  Unikalne kampanie: {df['campaign_id'].nunique()}")

    save_gold(df, "dim_campaign_period")


if __name__ == "__main__":
    build_dim_campaign_period()
