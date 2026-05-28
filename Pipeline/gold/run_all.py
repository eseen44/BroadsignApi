"""
Gold pipeline — orchestrator.

Uruchomienie:
    python Pipeline/gold/run_all.py

Wejście:  Data/bronze/*.parquet + Data/silver/*.parquet
Wyjście:  Data/gold/*.parquet  (star schema)

Tabele:
  dim_date             — kalendarz
  dim_campaign         — kampania + lineitem (ceny, daty, owner)
  dim_screen           — ekran + frame
  dim_player           — player + display_unit
  dim_content          — treść/kreacja
  fact_play_logs       — emisje (slim: klucze + miary)
  fact_campaign_budget — koszt dzienny per player per lineitem
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime
from Pipeline.gold.build_dims import build_dim_date, build_dim_campaign, build_dim_screen, build_dim_player, build_dim_content
from Pipeline.gold.build_fact_play_logs import build_fact_play_logs
from Pipeline.gold.build_fact_budget import build_fact_budget


STEPS = [
    ("dim_date",             build_dim_date),
    ("dim_campaign",         build_dim_campaign),
    ("dim_screen",           build_dim_screen),
    ("dim_player",           build_dim_player),
    ("dim_content",          build_dim_content),
    ("fact_play_logs",       build_fact_play_logs),
    ("fact_campaign_budget", build_fact_budget),
]


def run():
    start = datetime.now()
    print(f"{'='*55}")
    print(f"  Gold pipeline  {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*55}\n")

    results = {}

    for name, fn in STEPS:
        print(f"\n[{name}]")
        try:
            fn()
            results[name] = "OK"
        except Exception as e:
            import traceback
            print(f"  BLAD: {e}")
            traceback.print_exc()
            results[name] = f"FAIL: {e}"

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'='*55}")
    print(f"  Koniec ({elapsed}s)")
    print(f"{'='*55}")
    ok   = [k for k, v in results.items() if v == "OK"]
    fail = [k for k, v in results.items() if v != "OK"]
    for k in ok:
        print(f"  OK   {k}")
    for k in fail:
        print(f"  FAIL {k}: {results[k]}")

    return len(fail) == 0


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
