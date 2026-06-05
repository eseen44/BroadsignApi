"""
Join coverage validation — sprawdza pokrycie każdego kluczowego joinu w pipeline.
Uruchom po pełnym pipeline żeby zobaczyć co się nie mapuje.

Każda sekcja pokazuje:
  - Ile wierszy LEFT
  - Ile matchuje (RIGHT != NULL)
  - % pokrycia
  - Przykłady niematchwanych ID (top 10)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

BRONZE = Path(__file__).resolve().parent.parent / "Data" / "bronze"
SILVER = Path(__file__).resolve().parent.parent / "Data" / "silver"
GOLD   = Path(__file__).resolve().parent.parent / "Data" / "gold"


def check(label, left_ids, right_ids, sample_unmatched=10):
    left  = pd.Series(left_ids).dropna()
    right = set(pd.Series(right_ids).dropna().unique())
    matched   = left.isin(right).sum()
    unmatched = (~left.isin(right)).sum()
    pct = matched / len(left) * 100 if len(left) > 0 else 0
    status = "OK" if pct == 100 else ("WARN" if pct >= 95 else "FAIL")
    print(f"\n[{status}] {label}")
    print(f"  left={len(left):,}  matched={matched:,}  unmatched={unmatched:,}  coverage={pct:.1f}%")
    if unmatched > 0 and sample_unmatched > 0:
        missing = left[~left.isin(right)].value_counts().head(sample_unmatched)
        print(f"  Top unmatched IDs (id: count):")
        for uid, cnt in missing.items():
            print(f"    {uid}: {cnt:,} wierszy")
    return pct


results = {}

print("=" * 65)
print("  JOIN COVERAGE REPORT")
print("=" * 65)

# ------------------------------------------------------------------
# 1. SILVER: play_logs_enriched — kluczowe joiny
# ------------------------------------------------------------------
print("\n\n=== [1] SILVER: play_logs_enriched ===")

pl_enr = pd.read_parquet(SILVER / "play_logs_enriched.parquet",
                          columns=["CampID","DisplayUnitID","FrameID","contract_id",
                                   "reservation_name","screen_name","display_unit_name","proposal_name"])

res = pd.read_parquet(BRONZE / "ctrl_reservations.parquet", columns=["id"])
results["play_log->reservation"] = check(
    "play_log[CampID] -> ctrl_reservations[id]",
    pl_enr["CampID"].dropna(), res["id"]
)

du = pd.read_parquet(BRONZE / "ctrl_display_units.parquet", columns=["id"])
results["play_log->display_unit"] = check(
    "play_log[DisplayUnitID] -> ctrl_display_units[id]",
    pl_enr["DisplayUnitID"].dropna(), du["id"]
)

# Pokrycie nazw (po joinie)
matched_res = pl_enr["reservation_name"].notna().sum()
matched_du  = pl_enr["display_unit_name"].notna().sum()
matched_scr = pl_enr["screen_name"].notna().sum()
n = len(pl_enr)
print(f"\n  Post-join fill rates:")
print(f"  reservation_name:   {matched_res:,}/{n:,} ({matched_res/n*100:.1f}%)")
print(f"  display_unit_name:  {matched_du:,}/{n:,}  ({matched_du/n*100:.1f}%)")
print(f"  screen_name:        {matched_scr:,}/{n:,}  ({matched_scr/n*100:.1f}%)")
print(f"  proposal_name:      {pl_enr['proposal_name'].notna().sum():,}/{n:,}  ({pl_enr['proposal_name'].notna().mean()*100:.1f}%)")

# ------------------------------------------------------------------
# 2. GOLD: fact_play_logs — campaign_id mapping przez v22
# ------------------------------------------------------------------
print("\n\n=== [2] GOLD: fact_play_logs — reservation -> campaign ===")

fpl = pd.read_parquet(GOLD / "fact_play_logs.parquet",
                       columns=["reservation_id","campaign_id","line_item_id"])

v22 = pd.read_parquet(BRONZE / "ctrl_reservations_v22.parquet",
                       columns=["id","proposal_line_item_id"])

results["fact_pl->v22"] = check(
    "fact_play_logs[reservation_id] -> ctrl_reservations_v22[id]",
    fpl["reservation_id"].dropna(), v22["id"]
)

results["fact_pl->campaign"] = check(
    "fact_play_logs[campaign_id] -> not null (kampanie zmapowane)",
    fpl["reservation_id"],  # total
    fpl[fpl["campaign_id"].notna()]["reservation_id"]  # zmapowane
)

li_ids = pd.read_parquet(GOLD / "dim_line_item.parquet", columns=["line_item_id"])["line_item_id"]
results["fact_pl->lineitem"] = check(
    "fact_play_logs[line_item_id] -> dim_lineitem[line_item_id]",
    fpl["line_item_id"].dropna(), li_ids
)

# ------------------------------------------------------------------
# 3. GOLD: fact_campaign_budget — line_item -> player
# ------------------------------------------------------------------
print("\n\n=== [3] GOLD: fact_campaign_budget ===")

fcb = pd.read_parquet(GOLD / "fact_campaign_budget.parquet",
                       columns=["line_item_id","play_log_player_id","reservation_id"])

results["budget->lineitem"] = check(
    "fact_campaign_budget[line_item_id] -> dim_lineitem[line_item_id]",
    fcb["line_item_id"].dropna(), li_ids
)

dp = pd.read_parquet(GOLD / "dim_player.parquet", columns=["play_log_player_id"])
null_players = fcb["play_log_player_id"].isna().sum()
print(f"\n  [INFO] budget rows z NULL play_log_player_id: {null_players:,} ({null_players/len(fcb)*100:.1f}%)")
print(f"         = kampanie bez dopasowania w play_logs (fallback screen_count)")

results["budget->player"] = check(
    "fact_campaign_budget[play_log_player_id] -> dim_player (non-null only)",
    fcb["play_log_player_id"].dropna(), dp["play_log_player_id"]
)

# ------------------------------------------------------------------
# 4. GOLD: dim_lineitem -> reservation
# ------------------------------------------------------------------
print("\n\n=== [4] GOLD: dim_lineitem — reservation mapping ===")

dli = pd.read_parquet(GOLD / "dim_line_item.parquet",
                       columns=["line_item_id","reservation_id","status_name"])

results["lineitem->reservation"] = check(
    "dim_lineitem[reservation_id] -> ctrl_reservations[id] (non-null only)",
    dli["reservation_id"].dropna(), res["id"]
)

no_res = dli["reservation_id"].isna().sum()
print(f"\n  [INFO] dim_lineitem bez reservation_id: {no_res:,}/{len(dli):,} ({no_res/len(dli)*100:.1f}%)")
by_status = dli[dli["reservation_id"].isna()]["status_name"].value_counts()
print(f"  Rozkład statusów bez rezerwacji:")
for s, c in by_status.items():
    print(f"    {s}: {c}")

# ------------------------------------------------------------------
# PODSUMOWANIE
# ------------------------------------------------------------------
print("\n\n" + "=" * 65)
print("  PODSUMOWANIE")
print("=" * 65)
for name, pct in results.items():
    status = "OK  " if pct == 100 else ("WARN" if pct >= 95 else "FAIL")
    bar = "#" * int(pct / 5) + "." * (20 - int(pct / 5))
    print(f"  [{status}] [{bar}] {pct:6.1f}%  {name}")
