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

from Package.popstats.client import (
    get_session, list_playlog_files, list_resource_files,
    fetch_and_parse, fetch_resources_latest, filename_to_date,
)
from Pipeline.bronze.utils import append_parquet, upsert_parquet, load_cursors, save_cursor, save_parquet

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
    Sledzi pobrane pliki w _cursors.json.

    Strategia:
    - Jesli data pliku jest juz w play_logs.parquet -> oznacz jako pobrane bez downloadowania
      (pokrywa pliki historyczne obecne w CSV, ktre teraz pojawiaja sie jako .gz na serwerze)
    - Jesli data nowa -> pobierz, sparsuj, dodaj do parqueta
    """
    from Pipeline.bronze.utils import BRONZE_DIR
    import pandas as pd

    if session is None:
        session = get_session()

    cursors       = load_cursors()
    fetched_files = set(cursors.get(CURSORS_KEY, []))

    available = list_playlog_files(session)
    new_files  = [f for f in available if f not in fetched_files]

    if not new_files:
        print(f"  [popstats] Brak nowych plikow (dostepne: {len(available)}, pobrane: {len(fetched_files)}).")
        return {"new_files": 0, "new_rows": 0}

    # Wczytaj juz istniejace daty z parqueta (jesli istnieje)
    parquet_path = BRONZE_DIR / "play_logs.parquet"
    existing_dates: set = set()
    if parquet_path.exists():
        existing_dates = set(
            pd.read_parquet(parquet_path, columns=["DateEnd"])["DateEnd"]
            .astype(str).unique()
        ) - {"NaT", "nan", "None"}

    total_rows = 0
    skipped = 0
    downloaded = 0

    for filename in new_files:
        day = filename_to_date(filename)

        if day in existing_dates:
            # Data juz w parquecie (np. z historycznego CSV) — nie pobieraj
            print(f"    Pomijam {filename} — dzien {day} juz w parquecie")
            fetched_files.add(filename)
            skipped += 1
            save_cursor(CURSORS_KEY, sorted(fetched_files))
            continue

        print(f"    Pobieranie {filename} (dzien: {day})...")
        try:
            df = fetch_and_parse(session, filename)
            _, added = append_parquet(df, "play_logs", date_col="DateEnd")
            total_rows += added
            fetched_files.add(filename)
            downloaded += 1
            save_cursor(CURSORS_KEY, sorted(fetched_files))
        except Exception as e:
            print(f"    BLAD przy {filename}: {e}")

    print(f"  [popstats] Pobrane: {downloaded}, pominiete (juz w parquecie): {skipped}")
    return {"new_files": downloaded, "new_rows": total_rows}


# ---------------------------------------------------------------------------
# 3. Resources — slownik ID->Nazwa z popstats
# ---------------------------------------------------------------------------

def fetch_resources(session=None) -> int:
    """
    Pobiera najnowszy plik resources z popstats i zapisuje jako bronze.
    Zwraca liczbe rekordow.

    Plik resources zawiera slownik ID->Nazwa dla wszystkich obiektow uzywanych
    w play_logs:
      host         -> PlayerID       (= ctrl_players.claimed_resource_id)
      display_unit -> DisplayUnitID  (= ctrl_display_units.id)
      reservation  -> CampID         (= ctrl_reservations.id)
      content      -> AdCopyId       (= ctrl_content.id)
      skin         -> rendering template (zwykle ignorowany)
    """
    if session is None:
        session = get_session()

    df = fetch_resources_latest(session)
    upsert_parquet(df, "resources_latest", key_col="id")
    return len(df)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Play logs bronze ===")

    # 1. Import historyczny (pomijany jesli parquet juz istnieje)
    print("\n[1/3] Import historyczny...")
    import_historical()

    session = get_session()

    # 2. Dociagnij nowe pliki z popstats
    print("\n[2/3] Incremental fetch z popstats...")
    result = fetch_incremental(session)
    print(f"  Nowe pliki: {result['new_files']}, nowe wiersze: {result['new_rows']}")

    # 3. Resources
    print("\n[3/3] Resources (ID->Nazwa)...")
    n = fetch_resources(session)
    print(f"  Pobrano {n} zasobow.")
