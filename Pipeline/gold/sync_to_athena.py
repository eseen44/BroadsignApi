"""
Wgrywa parquety z Data/gold/ na S3, skad czyta je Athena (SWAT/BroadsignApi
wspolny data warehouse na AWS).

Samodzielny, kontrolowany krok -- NIE wpiety w run_all.py. Uruchamiany osobno:
    python3 -m Pipeline.gold.sync_to_athena
    python3 -m Pipeline.gold.sync_to_athena --dry-run
    python3 -m Pipeline.gold.sync_to_athena --tabele dim_campaign,fact_health

Zasady (ustalone 2026-07-30, patrz CLAUDE.md / pamięć aws_athena_access):
  - Lista tabel = STEPS z run_all.py (zrodlo prawdy), NIE glob po dysku --
    Data/gold/ zawiera tez reliktowe pliki (fact_fill, dim_screen) ktore
    NIE sa produktem pipeline'u i nie maja definicji w katalogu Glue.
  - Athena NIE ma UPSERT dla zwyklych tabel Parquet. Wgrywamy caly plik,
    zawsze pod TYM SAMYM kluczem S3 (nadpisanie jest atomowe w S3).
  - Katalog Glue (definicje tabel) zarzadzany centralnie przez IT (Terraform)
    -- ten skrypt NIE tworzy/nie zmienia tabel, tylko wrzuca pliki pod
    sciezki ktore juz istnieja. Zmiana schematu = nowy DDL do IT
    (Pipeline/gold/gen_athena_ddl.py w repo SWAT Refactor), nie ALTER TABLE stad.
  - Konwencja sciezki (potwierdzona przez IT): s3://<bucket>/<tabela>/data/<tabela>.parquet
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import argparse
from datetime import datetime, timezone

from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

from Pipeline.s3_utils import make_client, upload_retry

load_dotenv()

GOLD_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_DATA_BUCKET")

# Tabele ktore fizycznie leza w Data/gold/, ale NIE sa produktem run_all.py
# (relikt / eksperyment reczny) -- nigdy nie wysylac, nawet gdyby ktos podal
# je jawnie przez --tabele.
NIGDY_NIE_WYSYLAJ = {"fact_fill", "dim_screen"}


def wszystkie_tabele() -> list[str]:
    """Import lokalny -- run_all.py wola sync(), wiec import na poziomie modulu
    zrobilby cykl."""
    from Pipeline.gold.run_all import STEPS
    return [name for name, _ in STEPS]


def s3_key(tabela: str) -> str:
    return f"{tabela}/data/{tabela}.parquet"


def sync(tabele: list[str], dry_run: bool = False) -> dict:
    braki = [t for t in NIGDY_NIE_WYSYLAJ if t in tabele]
    if braki:
        raise ValueError(
            f"{braki} sa na liscie NIGDY_NIE_WYSYLAJ (relikt, nie produkt pipeline'u) "
            f"-- usun je z listy tabel do wgrania"
        )

    s3 = make_client()

    wyniki = {}
    print(f"\n=== Sync gold -> s3://{S3_BUCKET}/  ({'DRY RUN' if dry_run else 'na zywo'}) ===")
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n")

    for tabela in tabele:
        path = GOLD_DIR / f"{tabela}.parquet"
        if not path.exists():
            print(f"  POMINIETO {tabela:<24} -- brak pliku {path}")
            wyniki[tabela] = "BRAK PLIKU"
            continue

        key = s3_key(tabela)
        size_mb = path.stat().st_size / 1e6

        if dry_run:
            print(f"  [dry-run] {tabela:<24} {size_mb:8.2f} MB  -> s3://{S3_BUCKET}/{key}")
            wyniki[tabela] = f"DRY RUN ({size_mb:.2f} MB)"
            continue

        try:
            upload_retry(s3, path, S3_BUCKET, key, etykieta=tabela)
            print(f"  OK        {tabela:<24} {size_mb:8.2f} MB  -> s3://{S3_BUCKET}/{key}")
            wyniki[tabela] = f"OK ({size_mb:.2f} MB)"
        except ClientError as e:
            err = e.response["Error"]
            print(f"  BLAD      {tabela:<24} {err['Code']}: {err['Message'][:120]}")
            wyniki[tabela] = f"BLAD: {err['Code']}"
        except Exception as e:
            print(f"  BLAD      {tabela:<24} {type(e).__name__}: {str(e)[:120]}")
            wyniki[tabela] = f"BLAD: {type(e).__name__}"

    return wyniki


def main() -> bool:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="pokaz co by wyslal, nie wysylaj")
    ap.add_argument("--tabele", default=None,
                     help="lista po przecinku (domyslnie: wszystkie ze STEPS run_all.py)")
    args = ap.parse_args()

    if not all([AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET]):
        print("BLAD: brak konfiguracji AWS w .env (AWS_REGION, AWS_ACCESS_KEY_ID, "
              "AWS_SECRET_ACCESS_KEY, S3_DATA_BUCKET)")
        return False

    wszystkie = wszystkie_tabele()
    if args.tabele:
        tabele = [t.strip() for t in args.tabele.split(",") if t.strip()]
        nieznane = set(tabele) - set(wszystkie)
        if nieznane:
            print(f"BLAD: nieznane tabele (nie ma ich w STEPS run_all.py): {sorted(nieznane)}")
            return False
    else:
        tabele = wszystkie

    wyniki = sync(tabele, dry_run=args.dry_run)

    print(f"\n{'='*60}")
    fail = [t for t, w in wyniki.items() if not (w.startswith("OK") or w.startswith("DRY"))]
    for t, w in wyniki.items():
        print(f"  {t:<24} {w}")
    print(f"{'='*60}")

    return len(fail) == 0


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
