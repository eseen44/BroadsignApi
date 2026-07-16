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

# Oczekiwane emisje/godzine aktywna -- SKALIBROWANE per SubFormat na realnych
# danych (2026-07-15, is_serwisowy=0): rzeczywista petla rozni sie miedzy
# nosnikami (billboardy/Wroclaw maja ~90s petle = 2x wiecej emisji/h niz metro
# DMB/LiveLine/Triplay ktore maja ~180s). Zmierzone: sum_emisje/sum_active_hours
# per SubFormat na calej bazie fact_play_logs (is_serwisowy=0):
#   DMB=20.25 LiveLine=18.94 Triplay=19.31 Krakow=24.11 (~180s petla, 20/h)
#   B9D=40.22 B18D=39.14 B36D=40.43 Wroclaw=38.98 (~90s petla, 40/h)
#   StroerTV=30.09 (~120s petla)
#   Katowice=237.9 -- probka za mala (3162 aktywnych-h), zostaw default 20
#
# Rekalibracja 2026-07-16: sprawdzone TYLKO na kampaniach status_name="ended"
# (bez szumu z kampanii w trakcie). Wiekszosc formatow stabilna (Billboard/
# DMB/Triplay/Wroclaw +-1-2), ale LiveLine mial realny dryf: 18.94 (cala baza)
# vs 14.90 (tylko ended) -- kampanie w trakcie maja wyzsza gestosc niz
# zakonczone. Poprawione na 15 (z ended). Katowice pominiete (chaos, za mala
# probka niezaleznie od filtra -- 556 aktywnych-h na ended).
DEFAULT_SLOT_DURATION = 15  # fallback gdy brak slot_duration na linii
DEFAULT_EMISJE_PER_HOUR = 20

EXPECTED_EMISJE_PER_HOUR_BY_SUBFORMAT = {
    "DMB": 20, "LiveLine": 15, "Triplay": 20, "Kraków": 20,
    "B9D": 40, "B18D": 40, "B36D": 40, "Wrocław": 40,
    "StroerTV": 30,
}


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

    # SubFormat per player (replikuje kolumne DAX dim_player[SubFormat] z player_name,
    # tylko do wyboru wspolczynnika emisje/h -- nie zapisujemy tej kolumny do gold).
    dp = pd.read_parquet(GOLD_DIR / "dim_player.parquet", columns=["play_log_player_id", "player_name"])
    dp["play_log_player_id"] = pd.to_numeric(dp["play_log_player_id"], errors="coerce").astype("Int64")

    def _subformat(name):
        if not isinstance(name, str):
            return None
        n = name.lower()
        if "stroertv" in n: return "StroerTV"
        if "liveline" in n: return "LiveLine"
        if "b9d" in n: return "B9D"
        if "b18d" in n: return "B18D"
        if "b36d" in n: return "B36D"
        if "wroc" in n: return "Wrocław"
        if "krak" in n: return "Kraków"
        if "katowice" in n: return "Katowice"
        if "dmb" in n: return "DMB"
        return None

    dp["subfmt"] = dp["player_name"].apply(_subformat)
    fact["play_log_player_id"] = pd.to_numeric(fact["play_log_player_id"], errors="coerce").astype("Int64")
    fact = fact.merge(dp[["play_log_player_id", "subfmt"]], on="play_log_player_id", how="left")
    fact["emisje_per_hour"] = fact["subfmt"].map(EXPECTED_EMISJE_PER_HOUR_BY_SUBFORMAT).fillna(DEFAULT_EMISJE_PER_HOUR)
    fact = fact.drop(columns=["subfmt"])

    # Oczekiwane
    fact["emisje_expected"] = (fact["active_hours"] * fact["emisje_per_hour"]).round(2)
    fact["duration_expected_sec"] = (fact["emisje_expected"] * fact["slot_duration"]).round(2)
    fact = fact.drop(columns=["emisje_per_hour"])

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
