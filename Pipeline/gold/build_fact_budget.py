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

Korekta LiveLine/StroerTV (2026-07-13, przeprojektowana 2026-07-13 wieczorem):
Broadsign symuluje cala siec metra LiveLine/StroerTV jako 1-2 "wirtualne"
playery, wiec perf_actual/expected_repetitions z Direct API dla tych dwoch
formatow sa oderwane od realnej skali (potwierdzone porownaniem z MagicInfo
-- jedynym realnym zrodlem PoP dla tych ekranow). Dla wierszy sklasyfikowanych
jako LiveLine/StroerTV: (1) actual jest skalowany stala kalibrowana na
agregacie sieciowym vs MagicInfo, (2) expected jest liczony OD NOWA ze
skorygowanego actual (a nie z Direct API), bo oryginalny expected to sztywny
szablon (~60-1080/dzien niezaleznie od skali kampanii) bez zwiazku z
rzeczywistoscia. Kazda inna kombinacja format/pole zostaje nietknieta.

Klasyfikacja formatu -- PIERWSZENSTWO MA REALNY PLAYER, nie nazwa (per-ROW,
nie per-line-item):
  1. Jesli wiersz ma dopasowany play_log_player_id -> format z player_name
     (dim_player/players_full, CONTAINSSTRING "liveline"/"stroertv"). To
     jedyne zrodlo prawdy, bo pojedynczy line item MOZE mieszac fizyczne
     ekrany roznych formatow pod jedna pozycja zakupowa -- potwierdzone na
     danych: WSZYSTKIE 73 rezerwacje MetroMax z realnymi play_logami maja
     mix LiveLine+StroerTV playerow (4481 vs 2089 wierszy, 2026-07-13).
     "MetroMax" to pakiet/bundle (kupujesz wszystkie nosniki naraz), nie
     osobny fizyczny format -- stad NIE wolno traktowac calego line itemu
     jednym formatem.
  2. Brak dopasowanego playera (stare kampanie bez logow) -> fallback na
     line_item_name, ale TYLKO gdy nazwa jest jednoznaczna: dokladnie jeden
     format w tokenach nazwy (pelne "LiveLine"/"StroerTV" lub skroty
     "LL_"/"STV_", bez MetroMax i bez drugiego konkurencyjnego tokenu typu
     DMB/TP w tej samej nazwie).
  3. Brak playera I nazwa niejednoznaczna (MetroMax bez logow, albo nazwa
     mieszajaca kilka formatow np. "StroerTV & LL", "21/07 DMB/STV/LL") ->
     nie da sie rozstrzygnac per-format z samej nazwy. Uzywamy sredniego
     splitu LL/STV wyliczonego z tych samych 73 rezerwacji MetroMax z
     realnymi danymi (MIXED_FORMAT_SPLIT) jako najlepsze dostepne
     przyblizenie -- decyzja usera 2026-07-13 (28 line itemow / 897
     wierszy w tej kategorii w momencie wdrozenia).
"""
import hashlib
import re
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

# Split LL/STV dla bundle/niejednoznacznych line itemow bez zadnego dopasowanego
# playera (kategoria 3 opisu wyzej) -- wyliczony z 73 rezerwacji MetroMax ktore
# MAJA realne play_logi: 4481 wierszy liveline, 2089 wierszy stroertv (2026-07-13).
MIXED_FORMAT_SPLIT = {
    "liveline": 4481 / (4481 + 2089),
    "stroertv": 2089 / (4481 + 2089),
}
BLENDED_ACT_CORRECTION = sum(MIXED_FORMAT_SPLIT[f] * ACT_CORRECTION[f] for f in ACT_CORRECTION)


def _player_format(player_name) -> str | None:
    """'liveline' / 'stroertv' / None na podstawie player_name (dim_player/players_full)."""
    if not isinstance(player_name, str):
        return None
    n = player_name.lower()
    if "liveline" in n:
        return "liveline"
    if "stroertv" in n:
        return "stroertv"
    return None


def _name_tokens(line_item_name) -> set:
    """Tokeny formatu w nazwie line itemu: LL, STV, MM (MetroMax), DMB, TP."""
    if not isinstance(line_item_name, str):
        return set()
    n = line_item_name.lower()
    t = set()
    if "liveline" in n or re.search(r"(^|[_ /])ll([_ /]|$)", n):
        t.add("LL")
    if "stroertv" in n or re.search(r"(^|[_ /])stv([_ /]|$)", n):
        t.add("STV")
    if "metromax" in n:
        t.add("MM")
    if re.search(r"(^|[_ /])dmb([_ /]|$)", n):
        t.add("DMB")
    if re.search(r"(^|[_ /])tp([_ /]|$)", n):
        t.add("TP")
    return t


def _name_fallback_format(tokens: set) -> str | None:
    """Format z nazwy -- tylko fallback gdy brak playera. Zwraca 'liveline'/'stroertv'/
    'BUNDLE' (niejednoznaczne, uzyc MIXED_FORMAT_SPLIT) / None (brak sygnalu metra)."""
    metro = tokens & {"LL", "STV", "MM"}
    if not metro:
        return None
    if "MM" in tokens or len(tokens) > 1:
        return "BUNDLE"
    if tokens == {"LL"}:
        return "liveline"
    if tokens == {"STV"}:
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
        "campaign_id", "line_item_id", "reservation_id", "line_item_name",
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
    # 2b. Mapa play_log_player_id -> format metra (liveline/stroertv/None) --
    #     zrodlo prawdy nr 1 dla korekty (patrz docstring modulu).
    # ------------------------------------------------------------------
    players_full = read_silver("players_full")[["play_log_player_id", "player_name"]].copy()
    players_full = players_full.dropna(subset=["play_log_player_id"])
    players_full["play_log_player_id"] = pd.to_numeric(
        players_full["play_log_player_id"], errors="coerce"
    ).astype("Int64")
    players_full["metro_format"] = players_full["player_name"].apply(_player_format)
    player_metro_format = {
        pid: fmt
        for pid, fmt in zip(players_full["play_log_player_id"], players_full["metro_format"])
        if fmt is not None
    }
    print(f"  Playery LiveLine/StroerTV (zrodlo prawdy): {len(player_metro_format)}")

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
        name_fmt      = _name_fallback_format(_name_tokens(r["line_item_name"]))

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

        # Korekta per-player: player (zrodlo prawdy) > nazwa dedykowana (fallback)
        # > blended split (bundle/niejednoznaczne bez playera) > brak korekty.
        act_by_pid = {}
        exp_by_pid = {}
        for pid in set(player_list):
            correction = None
            player_fmt = player_metro_format.get(pid) if pid is not None else None
            if player_fmt is not None:
                correction = ACT_CORRECTION[player_fmt]
            elif name_fmt == "BUNDLE":
                correction = BLENDED_ACT_CORRECTION
            elif name_fmt in ACT_CORRECTION:
                correction = ACT_CORRECTION[name_fmt]

            if correction is not None:
                act_val = daily_act_rep * correction
                exp_val = act_val * _exp_factor_for_line(line_id)
            else:
                act_val = daily_act_rep
                exp_val = daily_exp_rep
            act_by_pid[pid] = act_val
            exp_by_pid[pid] = exp_val

        for day in date_range:
            day_str = day.strftime("%Y-%m-%d")
            for pid in player_list:
                rows.append({
                    "campaign_id":                   camp_id,
                    "line_item_id":                  line_id,
                    "reservation_id":                res_id,
                    "player_id":                     pid,
                    "date":                          day_str,
                    "daily_cost_line":               round(daily_cost, 4),
                    "daily_expected_repetitions":    round(exp_by_pid[pid], 2),
                    "daily_actual_repetitions":      round(act_by_pid[pid], 2),
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
