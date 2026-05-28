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

import re as _re

# Kody techniczne: A01, C11, TP, TP05, DMB, DMB03, CD itp.
# UWAGA: czyste akronimy bez cyfr (ONZ, PKP) NIE są kodami — są częścią nazwy stacji
_CODE_PATTERN = _re.compile(r'^(TP\d*|DMB\d*|CD|CA|[A-Z]{1,2}\d+[A-Z]?)$')
# Słowa kończące wyciąganie nazwy stacji (kierunki + "cała stacja")
# 'pónoc' — literówka w danych źródłowych (brak ł)
_STOP_WORDS   = {'północ', 'południe', 'wschód', 'zachód', 'polnoc', 'poludnie',
                 'pónoc',  # literówka w źródle
                 'cała', 'stacja'}

# Wzorce priorytetowe — sprawdzane w kolejności PRZED logiką stacji metra.
# Jeśli nazwa zawiera dany regexp, zwracamy etykietę bezpośrednio.
_PRIORITY_PATTERNS = [
    ("Wrocław",   _re.compile(r'Wrocław',   _re.IGNORECASE)),
    ("Kraków",    _re.compile(r'Kraków',    _re.IGNORECASE)),
    ("Katowice",  _re.compile(r'Katowice',  _re.IGNORECASE)),
    ("Lotnisko",  _re.compile(r'Lotnisko',  _re.IGNORECASE)),
    ("B9D",       _re.compile(r'B9D')),
    ("B18D",      _re.compile(r'B18D')),
    ("B36D",      _re.compile(r'B36D')),
]


def _lokalizacja(series: pd.Series) -> pd.Series:
    """
    Wyciąga lokalizację z nazwy display_unit / screen.

    Kolejność sprawdzania:
      1. Wzorce priorytetowe (miasta, formaty billboardów) — zwracają etykietę wprost
      2. Pipe-format: "B18D | Ulica/Adres | ID"  → część środkowa
      3. Underscore-format stacji metra: A01_Kabaty_TP05_południe → "Kabaty"
      4. Fallback: drugi wyraz (spacja)

    Przykłady:
      "...Wrocław..."                           → "Wrocław"
      "B18D | Plac Konstytucji | 123"           → "B18D"
      "B9D | Centrum | 456"                     → "B9D"
      A01_Kabaty_TP05_południe                  → "Kabaty"
      A10_TP_Pole_Mokotowskie_północ            → "Pole Mokotowskie"
      C13_DMB_Centrum_Nauk_Kopernik_cała_stacja → "Centrum Nauk Kopernik"
    """
    def _extract(val):
        if not isinstance(val, str):
            return None

        # 1. Wzorce priorytetowe
        for label, pattern in _PRIORITY_PATTERNS:
            if pattern.search(val):
                return label

        # 2. Pipe-format: billboardy "KOD | Ulica/Adres | ID"
        if " | " in val:
            parts = val.split(" | ")
            return parts[1].strip() if len(parts) >= 2 else None

        # 3. Underscore-format: stacje metra
        if "_" in val:
            parts = val.split("_")
            station_parts = []
            for part in parts[1:]:              # pomijamy [0] (kod sekcji)
                if not part:
                    continue
                low = part.lower()
                if low in _STOP_WORDS:
                    break                        # słowo kończące = stop
                if _CODE_PATTERN.match(part):
                    if station_parts:            # kod po nazwie = koniec
                        break
                    continue                     # kod przed nazwą = pomijamy
                station_parts.append(part)
            return " ".join(station_parts) if station_parts else None

        # 4. Fallback: spacja
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

    # Flaga nośników testowych / biurowych (łatwe filtrowanie w PBI)
    _test_pat = _re.compile(r'biuro|test', _re.IGNORECASE)
    df["is_test"] = df["display_unit_name"].apply(
        lambda v: bool(_test_pat.search(v)) if isinstance(v, str) else False
    )
    print(f"  Nosniki testowe (is_test=True): {df['is_test'].sum()}")

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
    print("[dim_date]");    build_dim_date()
    print("[dim_screen]");  build_dim_screen()
    print("[dim_player]");  build_dim_player()
    print("[dim_content]"); build_dim_content()


if __name__ == "__main__":
    build_all_dims()
