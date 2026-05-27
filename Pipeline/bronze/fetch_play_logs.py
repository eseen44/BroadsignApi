"""
Bronze fetch — play logi emisji (popstats.broadsign.com)

Dwie ścieżki:
  1. import_historical()  — jednorazowy import ALL_PLAYLOGS_AGGREGATED.csv
                            (historia zbudowana starym skryptem, bez contract_id)
  2. fetch_incremental()  — codzienne dociąganie nowych plików z popstats

Strategia: append_parquet() — dorzucamy tylko daty których jeszcze nie ma.
Kursor:    lista już pobranych plików w Data/bronze/_cursors.json
           → "play_logs_fetched_files": ["playlog-2026-05-26.txt", ...]
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd

from Package.popstats.client import get_session, list_playlog_files, fetch_and_parse, filename_to_date
from Pipeline.bronze.utils import append_parquet, load_cursors, save_cursor

HISTORICAL_CSV = (
    Path.home()
    / "OneDrive - Stroeer Poland Sp. z o.o"
    / "Pulpit"
    / "UbuntuSynch"
    / "ALL_PLAYLOGS_AGGREGATED.csv"
)

CURSORS_KEY = "play_logs_fetched_files"


# ---------------------------------------------------------------------------
# 1. Import historyczny (jednorazowy)
# ---------------------------------------------------------------------------

def import_historical(csv_path: Path = HISTORICAL_CSV) -> bool:
    """
    Importuje ALL_PLAYLOGS_AGGREGATED.csv jako punkt startowy play_logs.parquet.
    Uruchamiać tylko raz — jeśli play_logs.parquet już istnieje i ma dane,
    funkcja nic nie robi.
    """
    from Pipeline.bronze.utils import BRONZE_DIR

    parquet_path = BRONZE_DIR / "play_logs.parquet"
    if parquet_path.exists():
        existing = pd.read_parquet(parquet_path)
        if len(existing) > 0:
            print(f"  [historical] play_logs.parquet już istnieje ({len(existing)} wierszy) — pomijam.")
            return False

    if not csv_path.exists():
        print(f"  [historical] Nie znaleziono {csv_path} — pomijam.")
        return False

    print(f"  [historical] Wczytuję {csv_path} ...")
    df = pd.read_csv(csv_path, dtype={"DateEnd": str})

    # Stary CSV nie ma contract_id — dodajemy pusty
    if "contract_id" not in df.columns:
        df["contract_id"] = None

    # Upewnij się że kolumny są zgodne ze schematem
    expected = ["PlayerID", "DateEnd", "timeslot", "AdCopyId", "CampID",
                "FrameID", "DisplayUnitID", "contract_id", "Duration",
                "Impresje", "emisje", "timeslot_label"]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    df = df[expected]
    append_parquet(df, "play_logs", date_col="DateEnd")
    print(f"  [historical] Zaimportowano {len(df)} wierszy z historycznego CSV.")
    return True


# ---------------------------------------------------------------------------
# 2. Incremental fetch z popstats
# ---------------------------------------------------------------------------

def fetch_incremental(session=None) -> dict:
    """
    Pobiera z popstats tylko te pliki których jeszcze nie mamy.
    Śledzi pobrane pliki w _cursors.json.
    """
    if session is None:
        session = get_session()

    cursors      = load_cursors()
    fetched_files = set(cursors.get(CURSORS_KEY, []))

    available = list_playlog_files(session)
    new_files  = [f for f in available if f not in fetched_files]

    if not new_files:
        print(f"  [popstats] Brak nowych plików (dostępne: {len(available)}, pobrane: {len(fetched_files)}).")
        return {"new_files": 0, "new_rows": 0}

    print(f"  [popstats] Nowe pliki do pobrania: {new_files}")
    total_rows = 0

    for filename in new_files:
        day = filename_to_date(filename)
        print(f"    Pobieranie {filename} (dzień: {day})...")
        try:
            df = fetch_and_parse(session, filename)
            _, added = append_parquet(df, "play_logs", date_col="DateEnd")
            total_rows += added
            fetched_files.add(filename)
            save_cursor(CURSORS_KEY, sorted(fetched_files))
        except Exception as e:
            print(f"    BŁĄD przy {filename}: {e}")

    return {"new_files": len(new_files), "new_rows": total_rows}


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Play logs bronze ===")

    # 1. Import historyczny (pomijany jeśli parquet już istnieje)
    print("\n[1/2] Import historyczny...")
    import_historical()

    # 2. Dociągnij nowe pliki z popstats
    print("\n[2/2] Incremental fetch z popstats...")
    session = get_session()
    result  = fetch_incremental(session)

    print(f"\nGotowe: {result['new_files']} nowych plików, {result['new_rows']} nowych wierszy.")
