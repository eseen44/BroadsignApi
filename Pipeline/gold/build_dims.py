"""
Gold — budowanie wymiarów (dim_*).

dim_date    — pełny kalendarz ciągły (Time Intelligence ready)
dim_screen  — ekran + frame + Lokalizacja
dim_player  — player + display_unit + Lokalizacja
dim_content — treść/kreacja
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_bronze, read_silver, save_gold


# ---------------------------------------------------------------------------
# Helper: Lokalizacja (drugi wyraz nazwy, jak w Power Query)
# Text.BetweenDelimiters([Player], " ", " ", 0, 1)
# ---------------------------------------------------------------------------

def _lokalizacja(series: pd.Series) -> pd.Series:
    """Wyciąga drugi wyraz z nazwy (między 1. a 2. spacją)."""
    def _extract(val):
        if not isinstance(val, str):
            return None
        parts = val.split(" ")
        return parts[1] if len(parts) >= 2 else None
    return series.apply(_extract)


# ---------------------------------------------------------------------------
# Polskie nazwy miesięcy i dni (hardcoded — locale-independent)
# ---------------------------------------------------------------------------

PL_MONTH_FULL = {
    1: "Styczeń", 2: "Luty", 3: "Marzec", 4: "Kwiecień",
    5: "Maj", 6: "Czerwiec", 7: "Lipiec", 8: "Sierpień",
    9: "Wrzesień", 10: "Październik", 11: "Listopad", 12: "Grudzień",
}
PL_MONTH_SHORT = {
    1: "sty", 2: "lut", 3: "mar", 4: "kwi",
    5: "maj", 6: "cze", 7: "lip", 8: "sie",
    9: "wrz", 10: "paź", 11: "lis", 12: "gru",
}
# ISO weekday: 1=Pn … 7=Nd
PL_WEEKDAY_FULL = {
    1: "Poniedziałek", 2: "Wtorek", 3: "Środa", 4: "Czwartek",
    5: "Piątek", 6: "Sobota", 7: "Niedziela",
}
PL_WEEKDAY_SHORT = {
    1: "Pon", 2: "Wt", 3: "Śr", 4: "Czw",
    5: "Pt", 6: "Sob", 7: "Nd",
}


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------

def build_dim_date():
    """
    Ciągły kalendarz: 2025-01-01 → 2027-12-31
    (pełne lata — wymagane przez Time Intelligence w Power BI).

    Kolumny wzorowane na DAX Calendar używanym dotychczas,
    z polskimi nazwami miesięcy i dni.
    """
    dates = pd.date_range(start="2025-01-01", end="2027-12-31", freq="D")
    df = pd.DataFrame({"Date": dates})

    iso = df["Date"].dt.isocalendar()   # year, week, day (1=Pn … 7=Nd)
    iso_day  = iso["day"].astype(int)
    iso_week = iso["week"].astype(int)
    iso_year = iso["year"].astype(int)

    df["date_key"]        = df["Date"].dt.strftime("%Y-%m-%d")   # FK do fact tables
    df["Year"]            = df["Date"].dt.year
    df["Month Number"]    = df["Date"].dt.month
    df["Month Name"]      = df["Date"].dt.month.map(PL_MONTH_FULL)
    df["Short Month"]     = df["Date"].dt.month.map(PL_MONTH_SHORT)
    df["Quarter"]         = "Q" + df["Date"].dt.quarter.astype(str)
    df["Year-Month"]      = df["Date"].dt.strftime("%Y-%m")
    df["Weekday Name"]    = iso_day.map(PL_WEEKDAY_FULL)
    df["Short Weekday"]   = iso_day.map(PL_WEEKDAY_SHORT)
    df["Weekday Number"]  = iso_day                   # 1=Pn, 7=Nd (jak DAX WEEKDAY(...,2))
    df["ISO Year"]        = iso_year
    df["ISO Week"]        = iso_week
    df["ISO Week2"]       = ", tydzień: " + iso_week.astype(str)
    df["ISO Week3"]       = "Week: "      + iso_week.astype(str)
    df["YearWeek Index"]  = iso_year * 100 + iso_week
    df["is_weekend"]      = iso_day.isin([6, 7])
    df["Date"]            = df["Date"].dt.strftime("%Y-%m-%d")   # zostaw jako string

    # Walidacja ciągłości
    assert len(df) == (pd.Timestamp("2027-12-31") - pd.Timestamp("2025-01-01")).days + 1, \
        "dim_date nie jest ciągły!"

    save_gold(df, "dim_date")
    print(f"  Zakres: {df['date_key'].min()} do {df['date_key'].max()}, {len(df)} dni")


# ---------------------------------------------------------------------------
# dim_screen
# ---------------------------------------------------------------------------

def build_dim_screen():
    """
    Z silver.screens_full (screens × screens_frames_mapping).
    Klucz: frame_id — FrameID w play_logs.
    Lokalizacja: drugi wyraz screen_name.
    """
    df = read_silver("screens_full")

    keep = [
        "frame_id", "screen_id",
        "screen_name", "screen_address",
        "panel_id", "panel_uuid", "screen_uuid",
        "resolution", "orientation",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    df = df.drop_duplicates(subset=["frame_id"])

    df["Lokalizacja"] = _lokalizacja(df["screen_name"])

    for col in ["frame_id", "screen_id", "panel_id"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    save_gold(df, "dim_screen")


# ---------------------------------------------------------------------------
# dim_player
# ---------------------------------------------------------------------------

def build_dim_player():
    """
    Z silver.players_full (ctrl_players × ctrl_display_units).
    Klucz: play_log_player_id — PlayerID w play_logs.
    Lokalizacja: drugi wyraz display_unit_name (bliżej rzeczywistości niż player_name).
    """
    df = read_silver("players_full")

    keep = [
        "play_log_player_id", "player_id",
        "player_name", "hostname",
        "display_unit_id", "display_unit_name", "du_address",
        "timezone", "nscreens",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    df = df.drop_duplicates(subset=["play_log_player_id"])

    df["Lokalizacja"] = _lokalizacja(df["display_unit_name"])

    save_gold(df, "dim_player")


# ---------------------------------------------------------------------------
# dim_content
# ---------------------------------------------------------------------------

def build_dim_content():
    """
    Łączy ctrl_content z resources_latest (popstats).
    Klucz: content_id — AdCopyId w play_logs.
    """
    content = read_bronze("ctrl_content")[["id", "name", "domain_id"]].copy()
    content = content.rename(columns={"id": "content_id", "name": "content_name"})
    content["content_id"] = pd.to_numeric(content["content_id"], errors="coerce").astype("Int64")

    try:
        res = read_bronze("resources_latest")
        res_content = res[res["type"] == "content"][["id", "name"]].copy()
        res_content = res_content.rename(columns={"id": "content_id", "name": "popstats_name"})
        res_content["content_id"] = pd.to_numeric(
            res_content["content_id"], errors="coerce"
        ).astype("Int64")
        content = content.merge(res_content, on="content_id", how="left")
    except Exception:
        content["popstats_name"] = None

    content["content_name"] = content["content_name"].fillna(
        content.get("popstats_name", pd.Series(dtype=str))
    )
    content = content.drop(columns=["popstats_name"], errors="ignore")
    content = content.drop_duplicates(subset=["content_id"])

    save_gold(content, "dim_content")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def build_all_dims():
    print("[dim_date]");   build_dim_date()
    print("[dim_screen]"); build_dim_screen()
    print("[dim_player]"); build_dim_player()
    print("[dim_content]");build_dim_content()


if __name__ == "__main__":
    build_all_dims()
