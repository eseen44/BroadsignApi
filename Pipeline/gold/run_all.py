"""
Gold pipeline — orchestrator.

Uruchomienie:
    python Pipeline/gold/run_all.py

Wejscie:  Data/bronze/*.parquet + Data/silver/*.parquet
Wyjscie:  Data/gold/*.parquet  (star schema)

Tabele:
  dim_date          — kalendarz
  dim_campaign      — kampania (jedna na kampanie)
  dim_line_item     — line item + rezerwacja
  dim_player        — player + display_unit
  dim_content       — tresc/kreacja
  fact_play_logs    — emisje (slim: klucze + miary + campaign_id)
  fact_campaign_budget — koszt dzienny per player per lineitem
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from datetime import datetime
from Pipeline import status
from Pipeline.gold.build_dims import (
    build_dim_date, build_dim_screen, build_dim_player, build_dim_content
)
from Pipeline.gold.build_dim_campaign import build_dim_campaign
from Pipeline.gold.build_dim_line_item import build_dim_line_item
from Pipeline.gold.build_fact_play_logs import build_fact_play_logs
from Pipeline.gold.build_fact_budget import build_fact_budget
from Pipeline.gold.build_fact_health import build_fact_health
from Pipeline.gold.build_dim_campaign_period import build_dim_campaign_period


STEPS = [
    ("dim_date",             build_dim_date),
    ("dim_campaign",         build_dim_campaign),
    ("dim_line_item",        build_dim_line_item),
    ("dim_player",           build_dim_player),
    ("dim_content",          build_dim_content),
    ("fact_play_logs",       build_fact_play_logs),
    ("fact_campaign_budget", build_fact_budget),
    ("fact_health",          build_fact_health),
    ("dim_campaign_period",  build_dim_campaign_period),
]


ONEDRIVE_SYNC_DIR = Path("/dane/OneDrive/Pulpit/UbuntuSynch")


def sync_to_onedrive(gold_dir: Path) -> None:
    import shutil
    if not ONEDRIVE_SYNC_DIR.exists():
        print(f"  [sync] Pomijam — brak {ONEDRIVE_SYNC_DIR}")
        return
    copied = []
    for f in sorted(gold_dir.glob("*.parquet")):
        dest = ONEDRIVE_SYNC_DIR / f.name
        shutil.copy2(f, dest)
        copied.append(f.name)
    print(f"  [sync] Skopiowano {len(copied)} plików do UbuntuSynch: {copied}")


def run():
    start = datetime.now()
    print(f"{'='*55}")
    print(f"  Gold pipeline  {start:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*55}\n")

    results = {}

    for name, fn in STEPS:
        print(f"\n[{name}]")
        try:
            with status.mierz(name):
                fn()
            results[name] = "OK"
        except Exception as e:
            import traceback
            print(f"  BLAD: {e}")
            traceback.print_exc()
            results[name] = f"FAIL: {e}"

    status.dopisz_stany(results)
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

    if not fail:
        gold_dir = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"

        # OBIE sciezki dostawy sa KRYTYCZNE — decyzja usera 2026-08-05.
        # Power BI czyta dzis z Ateny przez ODBC, a UbuntuSynch -> SharePoint obsluguje
        # pozostalych odbiorcow. Niedostarczenie do KTOREJKOLWIEK z nich znaczy, ze ktos
        # patrzy na stare liczby, wiec musi dac niezerowy kod wyjscia.
        # Wczesniej S3 bylo non-critical (bo PBI czytal ze SharePointa) i jego awaria
        # przechodzila bez sladu w kodzie wyjscia — czyli cron raportowalby sukces przy
        # niedostarczonych danych. `upload_retry` w sync_to_athena nadal ponawia bledy
        # przejsciowe, wiec twardy FAIL tutaj znaczy realny problem, nie mrugnieta siec.
        dostawa: dict[str, str] = {}

        print("\n[sync_onedrive]")
        try:
            with status.mierz("sync_onedrive"):
                sync_to_onedrive(gold_dir)
            dostawa["sync_onedrive"] = "OK"
        except Exception as e:
            import traceback
            print(f"  BLAD: {e}")
            traceback.print_exc()
            dostawa["sync_onedrive"] = f"FAIL: {e}"

        print("\n[sync_s3]")
        try:
            with status.mierz("sync_s3"):
                from Pipeline.gold.sync_to_athena import sync, wszystkie_tabele
                wyniki = sync(wszystkie_tabele())
            zle = [t for t, w in wyniki.items() if not w.startswith("OK")]
            if zle:
                print(f"  BLAD: nie wyslano na S3: {zle}")
                dostawa["sync_s3"] = f"FAIL: nie wyslano {zle}"
            else:
                dostawa["sync_s3"] = "OK"
        except Exception as e:
            import traceback
            print(f"  BLAD: sync na S3 padl: {type(e).__name__}: {e}")
            traceback.print_exc()
            dostawa["sync_s3"] = f"FAIL: {type(e).__name__}: {e}"

        status.dopisz_stany(dostawa)
        results.update(dostawa)
        print()
        for k, v in dostawa.items():
            print(f"  {'OK  ' if v == 'OK' else 'FAIL'} {k}: {v}")
        fail += [k for k, v in dostawa.items() if v != "OK"]

    return len(fail) == 0


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
