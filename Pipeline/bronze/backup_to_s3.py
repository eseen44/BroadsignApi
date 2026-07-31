"""
Backup warstwy Bronze na S3.

PO CO: bronze zawiera dane, ktorych NIE DA SIE odtworzyc. popstats ma rolling
window ~2 miesiace, MagicInfo ~31 dni -- co z nich wypadnie, znika bezpowrotnie.
`play_logs.parquet` (~54 MB) to jedyna kopia historii emisji, a /dane na VM nie
jest przez nic backupowane (sprawdzone 2026-07-31: brak zadania w cronie).
Ten projekt juz raz stracil okno 2025-04-08..2025-06-25, a reszte historii
odzyskal tylko dlatego, ze user mial przypadkiem tarball na Pulpicie.

CZYM TO NIE JEST: to NIE sa tabele dla Atheny. Wrzucamy zwykle obiekty, bez
definicji w Glue -- IT nie musi niczego mapowac. Stad prefiks z 'staging'
w nazwie, zeby przy przegladaniu bucketa hurtowni bylo od razu widac, ze to
kopia zapasowa, a nie zrodlo dla warehouse'u.

Uruchamianie:
    python3 -m Pipeline.bronze.backup_to_s3
    python3 -m Pipeline.bronze.backup_to_s3 --dry-run
    python3 -m Pipeline.bronze.backup_to_s3 --snapshot     # dodatkowo kopia z data
    python3 -m Pipeline.bronze.backup_to_s3 --force        # zignoruj straznik rozmiaru
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import argparse
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from Pipeline.s3_utils import make_client, rozmiar_na_s3, upload_retry

load_dotenv()

BRONZE_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "bronze"
S3_BUCKET = os.getenv("S3_DATA_BUCKET")

# 'staging' w nazwie celowo -- to nie jest zrodlo dla hurtowni, tylko kopia
# zapasowa lezaca obok. IT zarzadza tym bucketem Terraformem i ma prawo sie
# zdziwic nieznanym prefiksem.
PREFIX = "staging_broadsign_bronze_backup"

# Nowy plik istotnie MNIEJSZY od kopii na S3 = podejrzenie, ze bronze zostal
# uszkodzony/obciety. Wtedy nadpisanie dobrej kopii bylo by utrata danych --
# ten sam wzorzec co guard na pusty df w bronze/utils.py::save_parquet.
PROG_KURCZENIA = 0.90


def backup(dry_run: bool = False, snapshot: bool = False, force: bool = False) -> dict:
    pliki = sorted(BRONZE_DIR.glob("*.parquet"))
    if not pliki:
        print(f"BLAD: brak plikow parquet w {BRONZE_DIR}")
        return {}

    s3 = make_client()
    dzis = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    wyniki = {}

    print(f"\n=== Backup bronze -> s3://{S3_BUCKET}/{PREFIX}/  "
          f"({'DRY RUN' if dry_run else 'na zywo'}) ===")
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC, plikow: {len(pliki)}\n")

    for path in pliki:
        nazwa = path.name
        key = f"{PREFIX}/latest/{nazwa}"
        rozmiar = path.stat().st_size
        mb = rozmiar / 1e6

        istniejacy = rozmiar_na_s3(s3, S3_BUCKET, key)
        if istniejacy and rozmiar < istniejacy * PROG_KURCZENIA and not force:
            print(f"  BLOKADA   {nazwa:<30} {mb:8.2f} MB  <- na S3 jest "
                  f"{istniejacy/1e6:.2f} MB. Skurczyl sie o "
                  f"{100*(1-rozmiar/istniejacy):.0f}% -- NIE nadpisuje "
                  f"(uzyj --force jesli to celowe)")
            wyniki[nazwa] = "ZABLOKOWANE (skurczenie)"
            continue

        if dry_run:
            byl = f"(na S3: {istniejacy/1e6:.2f} MB)" if istniejacy else "(nowy)"
            print(f"  [dry-run] {nazwa:<30} {mb:8.2f} MB  {byl}")
            wyniki[nazwa] = f"DRY RUN ({mb:.2f} MB)"
            continue

        try:
            upload_retry(s3, path, S3_BUCKET, key, etykieta=nazwa)
            if snapshot:
                upload_retry(s3, path, S3_BUCKET,
                             f"{PREFIX}/snapshots/{dzis}/{nazwa}", etykieta=nazwa)
            print(f"  OK        {nazwa:<30} {mb:8.2f} MB"
                  f"{'  + snapshot ' + dzis if snapshot else ''}")
            wyniki[nazwa] = f"OK ({mb:.2f} MB)"
        except Exception as e:
            print(f"  BLAD      {nazwa:<30} {type(e).__name__}: {str(e)[:110]}")
            wyniki[nazwa] = f"BLAD: {type(e).__name__}"

    return wyniki


def main() -> bool:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--snapshot", action="store_true",
                    help="obok latest/ zapisz tez kopie w snapshots/<data>/")
    ap.add_argument("--force", action="store_true",
                    help="nadpisz nawet gdy plik istotnie sie skurczyl")
    args = ap.parse_args()

    if not all([os.getenv("AWS_REGION"), os.getenv("AWS_ACCESS_KEY_ID"),
                os.getenv("AWS_SECRET_ACCESS_KEY"), S3_BUCKET]):
        print("BLAD: brak konfiguracji AWS w .env")
        return False

    wyniki = backup(args.dry_run, args.snapshot, args.force)
    if not wyniki:
        return False

    zle = [k for k, v in wyniki.items() if not (v.startswith("OK") or v.startswith("DRY"))]
    print(f"\n{'='*66}")
    for k, v in wyniki.items():
        print(f"  {k:<30} {v}")
    print(f"{'='*66}")
    print(f"  {len(wyniki) - len(zle)}/{len(wyniki)} OK")

    return not zle


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
