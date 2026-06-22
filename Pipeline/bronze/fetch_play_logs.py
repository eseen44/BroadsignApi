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
from Pipeline.bronze.utils import append_parquet, upsert_parquet, upsert_by_date, load_cursors, save_cursor, save_parquet

HISTORICAL_CSV = (
    Path.home()
    / "OneDrive - Stroeer Poland Sp. z o.o"
    / "Pulpit"
    / "UbuntuSynch"
    / "ALL_PLAYLOGS_AGGREGATED.csv"
)

CURSORS_KEY       = "play_logs_fetched_files"   # legacy — zachowany dla czystosci kursora
CURSORS_KEY_DATES = "play_logs_fetched_dates"   # dict {date_str -> source_filename}


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
    Pobiera z popstats pliki których data nie jest jeszcze w kursorze.

    Strategia: kursor po DACIE jest jedynym gate'em — nie sprawdzamy co jest
    w parquecie. Dzieki temu play_logs.parquet odzwierciedla dokladnie dane
    z popstats URL, bez domieszki innych zrodel (playlog_history, CSV).

    Cursor: play_logs_fetched_dates = {"2026-06-01": "playlog-2026-06-01.txt.gz", ...}
    Kazda data moze byc reprezentowana przez .txt lub .txt.gz — cursor sladzi date,
    nie nazwe pliku, wiec rotacja formatu nie powoduje ponownych downlooadow.
    """
    if session is None:
        session = get_session()

    cursors       = load_cursors()
    fetched_dates = cursors.get(CURSORS_KEY_DATES, {})   # dict: date_str -> filename

    available  = list_playlog_files(session)
    new_files  = [f for f in available if filename_to_date(f) not in fetched_dates]

    if not new_files:
        print(f"  [popstats] Brak nowych plikow (dostepne: {len(available)}, pobrane dat: {len(fetched_dates)}).")
        return {"new_files": 0, "new_rows": 0}

    total_rows = 0
    downloaded = 0
    errors     = 0

    for filename in new_files:
        day = filename_to_date(filename)
        print(f"    Pobieranie {filename} (dzien: {day})...")
        try:
            df = fetch_and_parse(session, filename)
            _, added = upsert_by_date(df, "play_logs", date_col="DateEnd", date=day)
            total_rows += added
            fetched_dates[day] = filename
            downloaded += 1
            save_cursor(CURSORS_KEY_DATES, fetched_dates)
        except Exception as e:
            print(f"    BLAD przy {filename}: {e}")
            errors += 1

    print(f"  [popstats] Pobrane: {downloaded}, bledy: {errors}, nowe wiersze: {total_rows}")
    return {"new_files": downloaded, "new_rows": total_rows, "errors": errors}


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
