"""
Silver pipeline — orchestrator.

Uruchomienie:
    python Pipeline/silver/run_all.py

Wejście:  Data/bronze/*.parquet
Wyjście:  Data/silver/*.parquet

Silver jest zawsze obliczany w całości (overwrite).
Uruchamiać po każdym przebiegu bronze pipeline.

Tabele:
  campaigns        — proposal_items × proposals (centralny stół BI)
  screens_full     — screens × screens_frames_mapping (frame_id -> screen)
  players_full     — ctrl_players × ctrl_display_units (player -> display_unit)
  screen_bridge    — (FrameID, DisplayUnitID) lookup zbudowany z play_logs
  play_logs_enr    — play_logs + nazwy rezerwacji/ekranów/kampanii Direct
  fill_rate_clean  — fill_rate + dodatkowe pola z proposal_items
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime

from Pipeline.silver.build_campaigns      import build_campaigns
from Pipeline.silver.build_screens        import build_screens
from Pipeline.silver.build_players        import build_players
from Pipeline.silver.build_screen_bridge  import build_screen_bridge
from Pipeline.silver.build_play_logs      import build_play_logs
from Pipeline.silver.build_fill_rate      import build_fill_rate
from Pipeline.silver.build_magicinfo_pop  import build_magicinfo_pop


STEPS = [
    ("campaigns",        build_campaigns,     "proposal_items × proposals"),
    ("screens_full",     build_screens,       "screens × screens_frames_mapping"),
    ("players_full",     build_players,       "ctrl_players × ctrl_display_units"),
    ("screen_bridge",    build_screen_bridge, "FrameID <-> DisplayUnitID z play_logs"),
    ("play_logs_enr",    build_play_logs,     "play_logs + nazwy"),
    ("fill_rate_clean",  build_fill_rate,     "fill_rate + proposal_items"),
    ("magicinfo_pop",    build_magicinfo_pop, "MagicInfo PoP metro (liveline/stroertv/triplay)"),
]


def run():
    start = datetime.now()
    print(f"{'='*55}")
    print(f"  Silver pipeline  {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*55}\n")

    results = {}

    for name, fn, desc in STEPS:
        print(f"\n[{name}]  {desc}")
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
