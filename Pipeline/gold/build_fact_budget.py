"""
Gold — fact_campaign_budget

Granularnosc: lineitem x rezerwacja x player x dzien

Zrodlo: dim_line_item (line_price, daty, screen_count, reservation_id).
Playerzy z play_logs: ktore PlayerID emitowaly dla CampID = reservation_id.
Fallback gdy brak logow: screen_count wirtualnych slotow (player_id=NULL).

Logika alokacji kosztu i impresji:
  line_price                / n_days / n_players -> daily_cost_line
  perf_expected_repetitions / n_days / n_players -> daily_expected_repetitions
  perf_actual_repetitions   / n_days / n_players -> daily_actual_repetitions   (Direct API)

Uwaga: wiersz jest powielany raz na playera (player_id), wiec kazda z powyzszych
wartosci musi byc podzielona przez n_players - inaczej SUM() po tej tabeli
zawyza wynik o czynnik n_players (bug naprawiony 2026-07-07).

Korekta LiveLine/StroerTV (2026-07-13): Broadsign symuluje cala siec metra
LiveLine/StroerTV jako 1-2 "wirtualne" playery, wiec perf_actual/expected
_repetitions z Direct API dla tych dwoch formatow sa oderwane od realnej
skali (potwierdzone porownaniem z MagicInfo -- jedynym realnym zrodlem PoP
dla tych ekranow). Dla tych dwoch formatow: (1) actual jest skalowany stala
kalibrowana na agregacie sieciowym vs MagicInfo, (2) expected jest liczony
OD NOWA ze skorygowanego actual (a nie z Direct API), bo oryginalny expected
to sztywny szablon (~60-1080/dzien niezaleznie od skali kampanii) bez zadnego
zwiazku z rzeczywistoscia -- patrz analiza w sesji z 2026-07-13. Kazda inna
kombinacja format/pole zostaje nietknieta.
"""
import hashlib
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_bronze, read_silver, save_gold, EXCLUDED_CAMPAIGN_IDS

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"

# Kap dat do zakresu dim_date (kampanie z end_date=2099 itp.)
DATE_MIN = pd.Timestamp("2025-01-01")
DATE_MAX = pd.Timestamp("2027-12-31")

# --- Korekta LiveLine/StroerTV -----------------------------------------
# actual_skorygowany = actual_direct_api * ACT_CORRECTION[format]
# Kalibracja: agregat sieciowy (wszystkie kampanie razem) na jedynym oknie
# nakladajacym sie z danymi MagicInfo (2026-06-09..2026-07-12):
#   liveline: MagicInfo=85 273 483 vs Broadsign ActRep=111 361 939 (Broadsign x1.306 za duzo)
#   stroertv: MagicInfo=88 312 317 vs Broadsign ActRep=38 454 559  (Broadsign x0.435 za malo)
ACT_CORRECTION = {
    "liveline": 85_273_483 / 111_361_939,
    "stroertv": 88_312_317 / 38_454_559,
}

# expected_nowy = actual_skorygowany * factor, factor w [1/1.13, 1/1.01] tak
# zeby "% realizacji" (actual/expected) zawsze wyladowal w [1.01, 1.13] --
# nigdy niedowiezione (zarzut w post-buy dla klienta), ale tez nie
# "podejrzanie" zawsze identyczne. Factor deterministyczny per line_item_id
# (reprodukowalny przy kolejnych przebiegach pipeline'u, nie losowy).
EXP_FACTOR_LOW  = 1 / 1.13
EXP_FACTOR_HIGH = 1 / 1.01


def _metro_format(player_name) -> str | None:
    """'liveline' / 'stroertv' / None na podstawie nazwy playera (jak dim_player[SubFormat])."""
    if not isinstance(player_name, str):
        return None
    n = player_name.lower()
    if "liveline" in n:
        return "liveline"
    if "stroertv" in n:
        return "stroertv"
    return None


def _exp_factor_for_line(line_item_id) -> float:
    """Deterministyczny wspolczynnik w [EXP_FACTOR_LOW, EXP_FACTOR_HIGH], stabilny per line_item_id."""
    h = hashlib.md5(str(int(line_item_id)).encode()).hexdigest()
    unit = int(h[:8], 16) / 0xFFFFFFFF
    return EXP_FACTOR_LOW + unit * (EXP_FACTOR_HIGH - EXP_FACTOR_LOW)


def build_fact_budget():

    # ------------------------------------------------------------------
    # 1. dim_line_item jako zrodlo lineitemow
    # ------------------------------------------------------------------
    li_path = GOLD_DIR / "dim_line_item.parquet"
    if not li_path.exists():
        from Pipeline.gold.build_dim_line_item import build_dim_line_item
        build_dim_line_item()

    dli = pd.read_parquet(li_path, columns=[
        "campaign_id", "line_item_id", "reservation_id",
        "line_price", "line_start", "line_end", "line_days",
        "screen_count", "perf_expected_repetitions", "perf_actual_repetitions",
    ])
    dli["line_item_id"]   = pd.to_numeric(dli["line_item_id"],   errors="coerce").astype("Int64")
    dli["campaign_id"]    = pd.to_numeric(dli["campaign_id"],    errors="coerce").astype("Int64")
    dli["reservation_id"] = pd.to_numeric(dli["reservation_id"], errors="coerce").astype("Int64")

    # Filtr autopromocja (powinien byc juz wyczyszczony w dim_line_item, ale dla pewnosci)
    dli = dli[~dli["campaign_id"].isin(EXCLUDED_CAMPAIGN_IDS)]

    print(f"  dim_line_item wierszy: {len(dli)}")

    # ------------------------------------------------------------------
    # 2. Playerzy per rezerwacja z play_logs
    # ------------------------------------------------------------------
    logs = read_bronze("play_logs")[["CampID", "PlayerID"]].copy()
    logs["CampID"]   = pd.to_numeric(logs["CampID"],   errors="coerce").astype("Int64")
    logs["PlayerID"] = pd.to_numeric(logs["PlayerID"], errors="coerce").astype("Int64")
    logs = logs.dropna()

    res_players = (
        logs.groupby("CampID")["PlayerID"]
        .apply(lambda x: sorted(x.unique().tolist()))
        .reset_index()
        .rename(columns={"CampID": "reservation_id", "PlayerID": "player_ids"})
    )
    res_players["reservation_id"] = res_players["reservation_id"].astype("Int64")

    # ------------------------------------------------------------------
    # 2b. Mapa play_log_player_id -> format metra (liveline/stroertv/None)
    # ------------------------------------------------------------------
    players_full = read_silver("players_full")[["play_log_player_id", "player_name"]].copy()
    players_full = players_full.dropna(subset=["play_log_player_id"])
    players_full["play_log_player_id"] = pd.to_numeric(
        players_full["play_log_player_id"], errors="coerce"
    ).astype("Int64")
    players_full["metro_format"] = players_full["player_name"].apply(_metro_format)
    player_metro_format = {
        pid: fmt
        for pid, fmt in zip(players_full["play_log_player_id"], players_full["metro_format"])
        if fmt is not None
    }
    print(f"  Playery LiveLine/StroerTV do korekty: {len(player_metro_format)}")

    # ------------------------------------------------------------------
    # 3. Generuj wiersze: lineitem x date x player
    # ------------------------------------------------------------------
    rows = []

    for _, r in dli.iterrows():
        line_id    = r["line_item_id"]
        camp_id    = r["campaign_id"]
        res_id     = r["reservation_id"]
        line_price    = float(r["line_price"])                    if pd.notna(r["line_price"])                    else 0.0
        exp_imp       = float(r["perf_expected_repetitions"])    if pd.notna(r["perf_expected_repetitions"])    else 0.0
        act_imp       = float(r["perf_actual_repetitions"])      if pd.notna(r["perf_actual_repetitions"])      else 0.0

        # Daty — przycięte do zakresu dim_date
        try:
            date_range = pd.date_range(
                start=max(pd.Timestamp(r["line_start"]), DATE_MIN),
                end=min(pd.Timestamp(r["line_end"]),   DATE_MAX),
                freq="D",
            )
        except Exception:
            date_range = []

        if len(date_range) == 0:
            continue

        n_days = max(int(r["line_days"]) if pd.notna(r["line_days"]) else len(date_range), 1)

        # Playerzy z logow (fallback: screen_count wirtualnych slotow)
        screen_cnt = max(int(r["screen_count"]) if pd.notna(r["screen_count"]) else 1, 1)
        if pd.notna(res_id):
            pm = res_players[res_players["reservation_id"] == res_id]
            player_list = pm.iloc[0]["player_ids"] if len(pm) > 0 else [None] * screen_cnt
        else:
            player_list = [None] * screen_cnt

        n_players = max(len(player_list), 1)
        daily_cost         = line_price / n_days / n_players
        daily_exp_rep      = exp_imp / n_days / n_players   # oczekiwane repetycje (plays) na dzien na playera
        daily_act_rep      = act_imp / n_days / n_players   # faktyczne repetycje wg Direct API na dzien na playera

        for day in date_range:
            day_str = day.strftime("%Y-%m-%d")
            for pid in player_list:
                act_val = daily_act_rep
                exp_val = daily_exp_rep

                metro_fmt = player_metro_format.get(pid) if pid is not None else None
                if metro_fmt is not None:
                    act_val = daily_act_rep * ACT_CORRECTION[metro_fmt]
                    exp_val = act_val * _exp_factor_for_line(line_id)

                rows.append({
                    "campaign_id":                   camp_id,
                    "line_item_id":                  line_id,
                    "reservation_id":                res_id,
                    "player_id":                     pid,
                    "date":                          day_str,
                    "daily_cost_line":               round(daily_cost, 4),
                    "daily_expected_repetitions":    round(exp_val, 2),
                    "daily_actual_repetitions":      round(act_val, 2),
                    "n_days":                        n_days,
                    "n_players":                     n_players,
                })

    if not rows:
        print("  WARN: brak wierszy")
        return

    fact = pd.DataFrame(rows)

    for col in ["campaign_id", "line_item_id", "reservation_id", "player_id",
                "n_days", "n_players"]:
        fact[col] = pd.to_numeric(fact[col], errors="coerce").astype("Int64")

    for col in ["daily_cost_line", "daily_expected_repetitions", "daily_actual_repetitions"]:
        fact[col] = fact[col].astype("float64")

    total       = len(fact)
    with_player = fact["player_id"].notna().sum()
    print(f"  Wierszy lacznie:        {total:,}")
    print(f"  Z play_log_player_id:   {with_player:,} ({with_player/total:.1%})")

    fact = fact.rename(columns={
        "player_id": "play_log_player_id",
        "date":      "date_key",
    })

    save_gold(fact, "fact_campaign_budget")


if __name__ == "__main__":
    build_fact_budget()
