"""
Gold — fact_health

Grain: play_log_player_id x date_key

Łączy fact_campaign_budget (co powinno grać wg planu) z fact_play_logs
(co faktycznie zaemitowało) na poziomie panel x dzień.

Incydent: panel miał zaplanowane kampanie ale zero emisji w play logach.

Kolumny:
  date_key                  - dzień
  play_log_player_id        - panel
  n_campaigns               - ile kampanii zaplanowanych na ten panel tego dnia
  n_line_items              - ile line itemów
  expected_impressions      - suma daily_expected_repetitions z budgetu
  actual_impressions        - suma Impresje z play_logs (0 jeśli brak)
  has_emission              - bool: czy cokolwiek grało
  incident                  - bool: plan > 0 ale emisje = 0
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import save_gold

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"


def build_fact_health():

    # ------------------------------------------------------------------
    # 1. Play logs najpierw — żeby znać zakres dat z faktycznymi danymi
    # ------------------------------------------------------------------
    print("  Wczytuję fact_play_logs...")
    logs = pd.read_parquet(
        GOLD_DIR / "fact_play_logs.parquet",
        columns=["date_key", "play_log_player_id", "Impresje"],
    )
    logs["play_log_player_id"] = logs["play_log_player_id"].astype("Int64")
    logs["date_key"] = pd.to_datetime(logs["date_key"]).dt.normalize()

    log_date_min = logs["date_key"].min()
    log_date_max = logs["date_key"].max()
    print(f"  Play log zakres: {log_date_min.date()} .. {log_date_max.date()}")

    logs_agg = (
        logs.groupby(["date_key", "play_log_player_id"])
        .agg(actual_impressions=("Impresje", "sum"))
        .reset_index()
    )
    print(f"  Play logs (panel x dzień): {len(logs_agg):,} wierszy")

    # ------------------------------------------------------------------
    # 2. Budget: które panele powinny grać każdego dnia
    #    Tylko wiersze z faktycznym player_id (nie null fallback)
    #    Przycinamy do zakresu dat z play logów — tylko tam gdzie mamy dane
    # ------------------------------------------------------------------
    print("  Wczytuję fact_campaign_budget...")
    budget = pd.read_parquet(
        GOLD_DIR / "fact_campaign_budget.parquet",
        columns=["date_key", "play_log_player_id", "campaign_id",
                 "line_item_id", "daily_expected_repetitions"],
    )
    budget = budget.dropna(subset=["play_log_player_id"]).copy()
    budget["play_log_player_id"] = budget["play_log_player_id"].astype("Int64")
    budget["date_key"] = pd.to_datetime(budget["date_key"]).dt.normalize()

    # Tylko daty w zakresie play logów MINUS pierwsze 60 dni (dane niekompletne)
    # i minus ostatni dzień (często niepełny)
    health_date_min = log_date_min + pd.Timedelta(days=60)
    health_date_max = log_date_max - pd.Timedelta(days=1)
    budget = budget[
        (budget["date_key"] >= health_date_min) &
        (budget["date_key"] <= health_date_max)
    ]
    print(f"  Budget zakres (po filtrze): {health_date_min.date()} .. {health_date_max.date()}")

    budget_agg = (
        budget.groupby(["date_key", "play_log_player_id"])
        .agg(
            n_campaigns=("campaign_id", "nunique"),
            n_line_items=("line_item_id", "nunique"),
            expected_impressions=("daily_expected_repetitions", "sum"),
        )
        .reset_index()
    )
    print(f"  Budget (panel x dzień): {len(budget_agg):,} wierszy")

    # ------------------------------------------------------------------
    # 3. Left join: budget LEFT JOIN play_logs
    #    Wiersze bez emisji w logach = potential incident
    # ------------------------------------------------------------------
    health = budget_agg.merge(
        logs_agg,
        on=["date_key", "play_log_player_id"],
        how="left",
    )

    health["actual_impressions"] = health["actual_impressions"].fillna(0).astype("float64")
    health["has_emission"] = health["actual_impressions"] > 0
    health["incident"]     = ~health["has_emission"]

    n_incidents = health["incident"].sum()
    n_total     = len(health)
    print(f"  Wierszy: {n_total:,}")
    print(f"  Incydenty (plan > 0, emisje = 0): {n_incidents:,} ({n_incidents/n_total:.1%})")

    save_gold(health, "fact_health")


if __name__ == "__main__":
    build_fact_health()
