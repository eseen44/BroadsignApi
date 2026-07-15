"""
Gold — fact_fill

Realizacja CZASOWA / fill kampanii, materializowana w gold (zamiast liczyc na
zywo w DAX -- SUMX+DISTINCTCOUNT po 15M wierszy play_logs bylo za ciezkie i
dawalo bledy; tu liczymy raz w pandas).

Grain: (line_item_id x play_log_player_id x date_key) -- ten sam co
fact_campaign_budget, z SubFormatem doczepionym przez playera.

Model zwalidowany 2026-07-15 na CN Kopernik:
  expected_emisje = active_hours * (3600 / LOOP_DURATION) * SLOTS_PER_LOOP
  gdzie LOOP_DURATION=180s, SLOTS_PER_LOOP=1  ->  20 emisji/godzine/player
  DMB Kopernik (fizyczny): expected 9260 vs actual 9033 = 97.5%.
  MetroMax (wagony/wirtualne LL/STV): 116% (resztka szumu wirtualnego ekranu).

Kolumny:
  active_hours          - liczba godzin (distinct timeslot) z >=1 emisja tego dnia
                          = realne okno pracy tej linii (peak-buy ~6h, caly dzien ~18h)
  emisje_actual         - faktyczne emisje (z play_logs)
  duration_actual_sec   - faktyczny czas emisji w sekundach (z play_logs)
  emisje_expected       - oczekiwane emisje = active_hours * 20 (1 slot/petle)
  duration_expected_sec - oczekiwany czas = emisje_expected * slot_duration linii
  is_serwisowy          - flaga (0/1/2) do filtrowania w PBI

Realizacja (w PBI) = SUM(duration_actual_sec) / SUM(duration_expected_sec).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import save_gold

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"

# Parametry pętli (zwalidowane na CN Kopernik 2026-07-15). Pojedyncza pętla trwa
# 180s; standardowo 1 slot kampanii na pętlę (saturation=1). Stad 3600/180=20
# oczekiwanych emisji na godzine na playera. Do ewentualnej rekalibracji per
# format, gdyby okazalo sie ze petla != 180 dla ktoregos nosnika.
LOOP_DURATION_SEC = 180
SLOTS_PER_LOOP = 1
DEFAULT_SLOT_DURATION = 15  # fallback gdy brak slot_duration na linii

EXPECTED_EMISJE_PER_HOUR = (3600 / LOOP_DURATION_SEC) * SLOTS_PER_LOOP  # = 20


def build_fact_fill():
    fpl_path = GOLD_DIR / "fact_play_logs.parquet"
    if not fpl_path.exists():
        from Pipeline.gold.build_fact_play_logs import build_fact_play_logs
        build_fact_play_logs()

    fpl = pd.read_parquet(fpl_path, columns=[
        "line_item_id", "play_log_player_id", "date_key",
        "timeslot", "emisje", "Duration", "is_serwisowy", "campaign_id",
    ])
    print(f"  fact_play_logs wierszy: {len(fpl):,}")

    # Agregacja do grain (line_item x player x dzien)
    grp = fpl.groupby(["line_item_id", "play_log_player_id", "date_key"], dropna=False)
    fact = grp.agg(
        active_hours=("timeslot", "nunique"),
        emisje_actual=("emisje", "sum"),
        duration_actual_sec=("Duration", "sum"),
        is_serwisowy=("is_serwisowy", "max"),
        campaign_id=("campaign_id", "first"),
    ).reset_index()
    print(f"  fact_fill wierszy (po agregacji): {len(fact):,}")

    # slot_duration z dim_line_item
    dli = pd.read_parquet(GOLD_DIR / "dim_line_item.parquet",
                          columns=["line_item_id", "slot_duration"])
    dli["line_item_id"] = pd.to_numeric(dli["line_item_id"], errors="coerce").astype("Int64")
    dli["slot_duration"] = pd.to_numeric(dli["slot_duration"], errors="coerce")
    dli = dli.drop_duplicates("line_item_id")

    fact["line_item_id"] = pd.to_numeric(fact["line_item_id"], errors="coerce").astype("Int64")
    fact = fact.merge(dli, on="line_item_id", how="left")
    fact["slot_duration"] = fact["slot_duration"].fillna(DEFAULT_SLOT_DURATION)

    # Oczekiwane
    fact["emisje_expected"] = (fact["active_hours"] * EXPECTED_EMISJE_PER_HOUR).round(2)
    fact["duration_expected_sec"] = (fact["emisje_expected"] * fact["slot_duration"]).round(2)

    # Typy
    for col in ["emisje_actual", "duration_actual_sec", "active_hours", "is_serwisowy"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").astype("Int64")
    for col in ["emisje_expected", "duration_expected_sec", "slot_duration"]:
        fact[col] = fact[col].astype("float64")

    tot_exp = fact["duration_expected_sec"].sum()
    tot_act = fact["duration_actual_sec"].sum()
    print(f"  Duration actual (h):    {tot_act/3600:,.0f}")
    print(f"  Duration expected (h):  {tot_exp/3600:,.0f}")
    print(f"  Realizacja globalna:    {tot_act/tot_exp*100:.1f}%" if tot_exp else "  brak expected")

    save_gold(fact, "fact_fill")


if __name__ == "__main__":
    build_fact_fill()
