"""
Jednorazowy import HISTORYCZNY slownika resources z backupu popstats.

Kontekst: popstats ma rolling window ~2 miesiace -- pliki `resources-YYYY-MM-DD.txt`
starsze niz okno znikaja bezpowrotnie. Codzienny `fetch_resources()` bierze tylko
NAJNOWSZY plik i upsertuje go po `id`, wiec bronze `resources_latest` narasta
dopiero od dnia, w ktorym pipeline zaczal chodzic. ID, ktore wypadly ze slownika
popstats PRZED tym dniem, nigdy do nas nie trafily.

Ten skrypt uzupelnia te dziure z archiwum (`resources_backup.tar.gz`, 347 plikow,
2025-06-27..2026-06-08).

Uruchamianie:
    python3 -m Pipeline.bronze.import_resources_history --dry-run
    python3 -m Pipeline.bronze.import_resources_history

    # inna sciezka do archiwum
    python3 -m Pipeline.bronze.import_resources_history --tarball /sciezka/do.tar.gz

Semantyka scalania -- ADD-ONLY, celowo:
  Wiersze ktore JUZ sa w `resources_latest` pochodza z codziennego fetcha, czyli
  sa SWIEZSZE niz archiwum. Nazwy rezerwacji w Broadsign bywaja edytowane, wiec
  historyczna nazwa nie moze nadpisac biezacej. Dodajemy WYLACZNIE id, ktorych
  w bronze nie ma. Dlatego NIE uzywamy `upsert_parquet` (ono nadpisuje po kluczu)
  -- patrz zasada "nigdy nie tracimy danych".

Skrypt jest idempotentny: drugi przebieg nie ma czego dodac.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import argparse
import gzip
import re
import tarfile
from datetime import datetime, timezone

import pandas as pd

from Pipeline.bronze.utils import BRONZE_DIR

DEFAULT_TARBALL = "/dane/BroadsignBackup/resources_backup.tar.gz"
OUT_NAME = "resources_latest"

# resources-2026-03-01.txt / resources-2026-03-01.txt.gz
FNAME_DATE = re.compile(r"resources-(\d{4}-\d{2}-\d{2})\.txt(\.gz)?$")


def _parse_lines(text: str, source_file: str) -> list[dict]:
    """Plik resources: TSV bez naglowka -- id, name, flag, type (+ pusty ogon)."""
    rows, malformed = [], 0
    for raw in text.splitlines():
        if not raw.strip():
            continue
        parts = raw.split("\t")
        if len(parts) < 4:
            malformed += 1
            continue
        rid, name, _flag, rtype = parts[0].strip(), parts[1], parts[2], parts[3].strip()
        if not rid.isdigit():
            malformed += 1
            continue
        rows.append({"id": int(rid), "name": name, "type": rtype,
                     "source_file": source_file})
    if malformed:
        print(f"    (pominieto {malformed} nieparsowalnych linii w {source_file})")
    return rows


def scan_tarball(tarball: Path) -> pd.DataFrame:
    """
    Zwraca DataFrame [id, name, type, source_file] -- jeden wiersz per id,
    nazwa z NAJNOWSZEGO pliku w ktorym id wystapilo.

    Iterujemy `for m in tar:` w kolejnosci strumienia -- tar.gz jest
    jednokierunkowy, losowy dostep przez getmember() to O(n^2).
    Kolejnosc strumienia NIE jest chronologiczna, wiec "najnowszy wygrywa"
    rozstrzygamy po dacie z nazwy pliku, nie po kolejnosci odczytu.
    """
    best: dict[int, tuple[str, str, str, str]] = {}   # id -> (file_date, name, type, source_file)
    all_dates: set[str] = set()
    n_files = 0

    with tarfile.open(tarball, "r|gz") as tar:
        for m in tar:
            if not m.isfile():
                continue
            fname = m.name.rsplit("/", 1)[-1]
            match = FNAME_DATE.search(fname)
            if not match:
                continue
            file_date = match.group(1)
            all_dates.add(file_date)

            fh = tar.extractfile(m)
            if fh is None:
                continue
            blob = fh.read()
            if fname.endswith(".gz"):
                blob = gzip.decompress(blob)
            text = blob.decode("utf-8", errors="replace")

            n_files += 1
            for row in _parse_lines(text, fname):
                rid = row["id"]
                prev = best.get(rid)
                if prev is None or file_date > prev[0]:
                    best[rid] = (file_date, row["name"], row["type"], fname)

    print(f"  Przeskanowano {n_files} plikow, {len(best)} unikalnych id")
    if all_dates:
        d = sorted(all_dates)
        print(f"  Zakres dat w archiwum: {d[0]} -> {d[-1]}")
    if not best:
        return pd.DataFrame(columns=["id", "name", "type", "source_file"])

    df = pd.DataFrame(
        [{"id": rid, "name": v[1], "type": v[2], "source_file": v[3]}
         for rid, v in best.items()]
    )
    win = sorted({v[0] for v in best.values()})
    print(f"  Nazwy pochodza z plikow z zakresu: {win[0]} -> {win[-1]}")
    return df


def main(tarball: str = DEFAULT_TARBALL, dry_run: bool = False) -> int:
    """Zwraca liczbe DODANYCH wierszy."""
    print("\n=== Import historyczny resources (backup popstats) ===")
    tar_path = Path(tarball)
    if not tar_path.exists():
        print(f"  BLAD: brak archiwum {tar_path}")
        return 0

    print(f"\n[1] Skan {tar_path.name} ({tar_path.stat().st_size / 1e6:.1f} MB)...")
    hist = scan_tarball(tar_path)
    if hist.empty:
        print("  Archiwum nie zawiera parsowalnych plikow resources -- koniec.")
        return 0

    path = BRONZE_DIR / f"{OUT_NAME}.parquet"
    print(f"\n[2] Scalanie z {path.name}...")
    if not path.exists():
        print(f"  Brak {path.name} -- caly historyczny slownik idzie jako nowy.")
        existing = pd.DataFrame(columns=["id"])
    else:
        existing = pd.read_parquet(path)
        print(f"  Bronze ma teraz {len(existing)} wierszy")

    existing_ids = set(pd.to_numeric(existing["id"], errors="coerce").dropna().astype("int64"))
    new = hist[~hist["id"].isin(existing_ids)].copy()

    print(f"  Historyczny slownik: {len(hist)} id")
    print(f"  Pokrywa sie z bronze: {len(hist) - len(new)}  (zostawiam wersje z bronze -- swiezsza)")
    print(f"  DO DODANIA (brak w bronze): {len(new)}")
    if len(new):
        print("\n  Rozbicie nowych po typie:")
        for t, n in new["type"].value_counts().items():
            print(f"    {t:14s} {n}")

    if not len(new):
        print("\n  Nic do dodania -- bronze juz zawiera caly historyczny slownik.")
        return 0

    if dry_run:
        print("\n  [DRY RUN] Nie zapisuje. Przyklady nowych wierszy:")
        for _, r in new.head(10).iterrows():
            print(f"    {r['id']:<12} {r['type']:<13} {r['name'][:60]}")
        return 0

    new["_fetched_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    merged = pd.concat([existing, new], ignore_index=True)

    # Sanity: add-only nie moze niczego zgubic ani zduplikowac klucza.
    assert len(merged) == len(existing) + len(new), "scalanie zgubilo wiersze"
    assert not merged["id"].duplicated().any(), "duplikat id po scaleniu"

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(path, index=False, engine="pyarrow")
    print(f"\n  -> [add-only] +{len(new)} wierszy -> {path.name} ({len(merged)} razem)")
    return len(new)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tarball", default=DEFAULT_TARBALL, help="sciezka do resources_backup.tar.gz")
    ap.add_argument("--dry-run", action="store_true", help="pokaz co by dodal, nie zapisuj")
    args = ap.parse_args()
    main(tarball=args.tarball, dry_run=args.dry_run)
    print("\n=== Koniec ===")
