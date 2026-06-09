import requests
from bs4 import BeautifulSoup
import os, re, json
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path

# ── KONFIGURACJA ──────────────────────────────────────────────────────────────
URL_BASE  = "https://popstats.broadsign.com/stroer_polska/"
USERNAME  = "stroer_polska"
PASSWORD  = "st9073f8b4"

BASE_DIR          = Path("/dane/Broadsign_Logs")
TEMP_DIR          = Path("/tmp/broadsign_dl")
PLAYLOG_PARQUET   = BASE_DIR / "playlog_history.parquet"
RESOURCES_PARQUET = BASE_DIR / "resources_latest.parquet"
MANIFEST_FILE     = BASE_DIR / "processed_manifest.json"
FINAL_DIR         = Path("/dane/OneDrive/Pulpit/UbuntuSynch")

TEMP_DIR.mkdir(parents=True, exist_ok=True)

DATE_RE = re.compile(r"resources-(\d{4}-\d{2}-\d{2})\.txt$", re.IGNORECASE)

# ── MANIFEST ──────────────────────────────────────────────────────────────────
def load_manifest() -> dict:
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    return {"playlog": {}, "resources": {}}

def save_manifest(manifest: dict):
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=2)

# ── SERWER ────────────────────────────────────────────────────────────────────
def get_server_files() -> tuple[requests.Session, dict]:
    """Zwraca sesję + dict {nazwa_pliku: server_size}."""
    session = requests.Session()
    session.auth = (USERNAME, PASSWORD)

    print("📥 Pobieram listę plików z serwera...")
    response = session.get(URL_BASE)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    files = {}

    for link in soup.find_all("a"):
        href = link.get("href", "").strip("/\\")
        if not href or href.startswith("?"):
            continue
        lowered = href.lower()
        if not (lowered.endswith(".txt") and ("playlog" in lowered or "resource" in lowered)):
            continue
        try:
            head = session.head(URL_BASE + href)
            head.raise_for_status()
            files[href] = int(head.headers.get("Content-Length", 0))
        except Exception as e:
            print(f"  ⚠️  Nie mogę sprawdzić rozmiaru {href}: {e}")

    return session, files

def download_file(session: requests.Session, fname: str, target: Path):
    url = URL_BASE + fname
    with session.get(url, stream=True) as r:
        r.raise_for_status()
        with open(target, "wb") as f:
            for chunk in r.iter_content(chunk_size=65_536):
                f.write(chunk)

# ── PLAYLOG ───────────────────────────────────────────────────────────────────
PLAYLOG_GROUPBY = [
    "PlayerID", "DateEnd", "timeslot",
    "AdCopyId", "CampID", "FrameID", "DisplayUnitID",
]

def parse_playlog_txt(txt_path: Path) -> pd.DataFrame:
    """
    Czyta jeden plik playlog .txt (TSV, bez nagłówka) i zwraca
    zaagregowany DataFrame — identyczna logika jak w v3.
    """
    parts = []

    for chunk in pd.read_csv(txt_path, sep="\t", header=None, chunksize=500_000):
        for col in [0, 2, 3, 5, 6, 7, 8]:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

        if 1 in chunk.columns:
            chunk[1] = pd.to_datetime(chunk[1], errors="coerce")

        chunk = chunk.drop(columns=[c for c in [4, 9, 10, 11, 12] if c in chunk.columns],
                           errors="ignore")
        chunk = chunk.rename(columns={
            0: "PlayerID", 1: "DateEndTime", 2: "Duration",
            3: "AdCopyId",  5: "CampID",      6: "FrameID",
            7: "DisplayUnitID", 8: "Impresje",
        })

        chunk["DateEnd"]  = chunk["DateEndTime"].dt.strftime("%Y-%m-%d")
        chunk["timeslot"] = chunk["DateEndTime"].dt.hour
        chunk["emisje"]   = 1

        agg = chunk.groupby(PLAYLOG_GROUPBY, as_index=False).agg(
            Duration=("Duration", "sum"),
            Impresje=("Impresje", "sum"),
            emisje=("emisje",   "sum"),
        )
        parts.append(agg)

    if not parts:
        return pd.DataFrame()

    result = pd.concat(parts, ignore_index=True)
    result = result.groupby(PLAYLOG_GROUPBY, as_index=False).agg(
        Duration=("Duration", "sum"),
        Impresje=("Impresje", "sum"),
        emisje=("emisje",   "sum"),
    )
    result["timeslot_label"] = result["timeslot"].apply(
        lambda h: f"{int(h):02d}:00-{(int(h)+1)%24:02d}:00" if pd.notnull(h) else None
    )
    return result


def union_playlog(new_df: pd.DataFrame, dates_to_replace: list) -> int:
    """
    Wczytuje istniejący parquet, usuwa wiersze dla podanych dat
    (re-download), dołącza nowe dane, zapisuje. Zwraca liczbę wierszy.
    """
    if PLAYLOG_PARQUET.exists():
        existing = pd.read_parquet(PLAYLOG_PARQUET)
        if dates_to_replace:
            existing = existing[~existing["DateEnd"].isin(dates_to_replace)]
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.groupby(PLAYLOG_GROUPBY, as_index=False).agg(
            Duration=("Duration", "sum"),
            Impresje=("Impresje", "sum"),
            emisje=("emisje",   "sum"),
        )
        combined["timeslot_label"] = combined["timeslot"].apply(
            lambda h: f"{int(h):02d}:00-{(int(h)+1)%24:02d}:00" if pd.notnull(h) else None
        )
    else:
        combined = new_df

    combined.to_parquet(PLAYLOG_PARQUET, index=False)
    return len(combined)


# ── RESOURCES ─────────────────────────────────────────────────────────────────
def union_resources(fname: str, new_df: pd.DataFrame) -> int:
    """
    Scala nowe dane z istniejącym parquetem.
    Zasada last-write-wins: dla każdego id zostaje wpis z najnowszą file_date.
    Zwraca liczbę unikalnych id po scaleniu.
    """
    if RESOURCES_PARQUET.exists():
        existing = pd.read_parquet(RESOURCES_PARQUET)
        # usuń stare wiersze z tego samego pliku (obsługa re-downloadu)
        existing = existing[existing["src_file"] != fname]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined = combined.sort_values(["id", "file_date"])
    combined = combined.drop_duplicates(subset=["id"], keep="last").copy()
    combined.to_parquet(RESOURCES_PARQUET, index=False)
    return len(combined)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"Wykonano: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"{'='*60}\n")

    manifest = load_manifest()
    session, server_files = get_server_files()
    print(f"✅ Znaleziono {len(server_files)} plików na serwerze.\n")

    pl_new = pl_upd = pl_skip = 0
    rs_new = rs_upd = rs_skip = 0

    # ── PLAYLOG ───────────────────────────────────────────────────────────────
    print("📊 Przetwarzam playlog...")

    for fname in sorted(f for f in server_files if "playlog" in f.lower()):
        server_size    = server_files[fname]
        recorded_size  = manifest["playlog"].get(fname)

        if recorded_size == server_size:
            pl_skip += 1
            continue

        is_update = recorded_size is not None
        label     = "aktualizacja" if is_update else "nowy"
        print(f"  ⬇️  {fname} ({label}, {server_size/1e6:.1f} MB)...")

        tmp = TEMP_DIR / fname
        try:
            download_file(session, fname, tmp)
            new_df = parse_playlog_txt(tmp)

            if new_df.empty:
                print(f"  ⚠️  Pusty wynik dla {fname}, pomijam.")
                tmp.unlink(missing_ok=True)
                continue

            dates_to_replace = list(new_df["DateEnd"].unique()) if is_update else []
            total = union_playlog(new_df, dates_to_replace)

            manifest["playlog"][fname] = server_size
            save_manifest(manifest)
            tmp.unlink(missing_ok=True)

            print(f"  ✅ Gotowe — parquet ma teraz {total:,} wierszy.")
            pl_upd += is_update
            pl_new += not is_update

        except Exception as e:
            print(f"  ❌ Błąd przy {fname}: {e}")
            tmp.unlink(missing_ok=True)

    print(f"\n  Playlog: {pl_new} nowych, {pl_upd} zaktualizowanych, {pl_skip} pominiętych.")

    # ── RESOURCES ─────────────────────────────────────────────────────────────
    print("\n📊 Przetwarzam resources...")

    for fname in sorted(f for f in server_files if DATE_RE.search(f)):
        server_size   = server_files[fname]
        recorded_size = manifest["resources"].get(fname)

        if recorded_size == server_size:
            rs_skip += 1
            continue

        m         = DATE_RE.search(fname)
        file_date = m.group(1)            # string YYYY-MM-DD
        is_update = recorded_size is not None
        label     = "aktualizacja" if is_update else "nowy"
        print(f"  ⬇️  {fname} ({label})...")

        tmp = TEMP_DIR / fname
        try:
            download_file(session, fname, tmp)

            raw = pd.read_csv(
                tmp, sep="\t", header=0,
                usecols=[0, 1], dtype={0: "string", 1: "string"},
                engine="python",
            )
            raw = raw.rename(columns={raw.columns[0]: "id", raw.columns[1]: "val"})
            raw["file_date"] = file_date
            raw["src_file"]  = fname

            total = union_resources(fname, raw)

            manifest["resources"][fname] = server_size
            save_manifest(manifest)
            tmp.unlink(missing_ok=True)

            print(f"  ✅ Gotowe — resources ma teraz {total:,} unikalnych id.")
            rs_upd += is_update
            rs_new += not is_update

        except Exception as e:
            print(f"  ❌ Błąd przy {fname}: {e}")
            tmp.unlink(missing_ok=True)

    print(f"\n  Resources: {rs_new} nowych, {rs_upd} zaktualizowanych, {rs_skip} pominiętych.")

    # ── EKSPORT CSV DO ONEDRIVE (backward compatibility) ──────────────────────
    print(f"\n📤 Eksportuję CSV do OneDrive...")

    if PLAYLOG_PARQUET.exists():
        df = pd.read_parquet(PLAYLOG_PARQUET)
        out = FINAL_DIR / "ALL_PLAYLOGS_AGGREGATED.csv"
        df.to_csv(out, index=False, encoding="utf-8")
        print(f"  ✅ {out.name}  ({len(df):,} wierszy)")

    if RESOURCES_PARQUET.exists():
        df = pd.read_parquet(RESOURCES_PARQUET)
        out = FINAL_DIR / "ALL_RESOURCES_LATEST.csv"
        df[["id", "val"]].to_csv(out, sep="\t", index=False, encoding="utf-8")
        print(f"  ✅ {out.name}  ({len(df):,} unikalnych id)")

    print(f"\n{'='*60}")
    print("✅ ZAKOŃCZONO")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
