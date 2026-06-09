"""
migrate_to_parquet.py
=====================
Jednorazowy skrypt migracyjny.

Czyta LOKALNE pliki .txt z /dane/Broadsign_Logs/playlog/ i resources/,
buduje playlog_history.parquet, resources_latest.parquet
oraz processed_manifest.json.

NIE usuwa plików .txt — to robi operator po weryfikacji.

Uruchomienie:
    python3 /dane/Broadsign_Logs/migrate_to_parquet.py

Bezpieczny do wielokrotnego uruchomienia (--resume pomija już przetworzone).
"""

import re, json, sys
import pandas as pd
from datetime import datetime
from pathlib import Path

BASE_DIR          = Path("/dane/Broadsign_Logs")
PLAYLOG_DIR       = BASE_DIR / "playlog"
RESOURCES_DIR     = BASE_DIR / "resources"
PLAYLOG_PARQUET   = BASE_DIR / "playlog_history.parquet"
RESOURCES_PARQUET = BASE_DIR / "resources_latest.parquet"
MANIFEST_FILE     = BASE_DIR / "processed_manifest.json"

DATE_RE = re.compile(r"resources-(\d{4}-\d{2}-\d{2})\.txt$", re.IGNORECASE)

PLAYLOG_GROUPBY = [
    "PlayerID", "DateEnd", "timeslot",
    "AdCopyId", "CampID", "FrameID", "DisplayUnitID",
]

# ── MANIFEST ──────────────────────────────────────────────────────────────────
def load_manifest() -> dict:
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    return {"playlog": {}, "resources": {}}

def save_manifest(m: dict):
    with open(MANIFEST_FILE, "w") as f:
        json.dump(m, f, indent=2)

# ── PLAYLOG ───────────────────────────────────────────────────────────────────
def parse_playlog_txt(txt_path: Path) -> pd.DataFrame:
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


def migrate_playlogs(manifest: dict, resume: bool):
    files = sorted(PLAYLOG_DIR.glob("playlog-*.txt"))
    total = len(files)
    print(f"📂 Znaleziono {total} plików playlog .txt\n")

    done = 0
    for i, txt_path in enumerate(files, 1):
        fname = txt_path.name

        if resume and fname in manifest["playlog"]:
            print(f"  [{i:3}/{total}] ⏭️  {fname} — już w manifeście, pomijam.")
            done += 1
            continue

        size = txt_path.stat().st_size
        print(f"  [{i:3}/{total}] ⬇️  {fname}  ({size/1e6:.1f} MB)...", end=" ", flush=True)

        try:
            new_df = parse_playlog_txt(txt_path)
            if new_df.empty:
                print("⚠️  pusty, pomijam.")
                continue

            # union z istniejącym parquetem
            if PLAYLOG_PARQUET.exists():
                existing = pd.read_parquet(PLAYLOG_PARQUET)
                # usuń wiersze dla dat z tego pliku (idempotentność)
                dates = list(new_df["DateEnd"].unique())
                existing = existing[~existing["DateEnd"].isin(dates)]
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
            manifest["playlog"][fname] = size
            save_manifest(manifest)
            done += 1
            print(f"✅  parquet: {len(combined):,} wierszy łącznie.")

        except Exception as e:
            print(f"❌  BŁĄD: {e}")

    print(f"\n✅ Playlog: {done}/{total} plików przetworzonych.\n")


# ── RESOURCES ─────────────────────────────────────────────────────────────────
def migrate_resources(manifest: dict, resume: bool):
    files = sorted(RESOURCES_DIR.glob("resources-*.txt"))
    total = len(files)
    print(f"📂 Znaleziono {total} plików resources .txt\n")

    done = 0
    for i, txt_path in enumerate(files, 1):
        fname = txt_path.name
        m     = DATE_RE.search(fname)
        if not m:
            print(f"  [{i:3}/{total}] ⚠️  {fname} — nierozpoznana nazwa, pomijam.")
            continue

        if resume and fname in manifest["resources"]:
            print(f"  [{i:3}/{total}] ⏭️  {fname} — już w manifeście, pomijam.")
            done += 1
            continue

        file_date = m.group(1)
        size      = txt_path.stat().st_size
        print(f"  [{i:3}/{total}] ⬇️  {fname}  (data={file_date})...", end=" ", flush=True)

        try:
            raw = pd.read_csv(
                txt_path, sep="\t", header=0,
                usecols=[0, 1], dtype={0: "string", 1: "string"},
                engine="python",
            )
            raw = raw.rename(columns={raw.columns[0]: "id", raw.columns[1]: "val"})
            raw["file_date"] = file_date
            raw["src_file"]  = fname

            if RESOURCES_PARQUET.exists():
                existing = pd.read_parquet(RESOURCES_PARQUET)
                existing = existing[existing["src_file"] != fname]
                combined = pd.concat([existing, raw], ignore_index=True)
            else:
                combined = raw

            combined = combined.sort_values(["id", "file_date"])
            combined = combined.drop_duplicates(subset=["id"], keep="last").copy()
            combined.to_parquet(RESOURCES_PARQUET, index=False)

            manifest["resources"][fname] = size
            save_manifest(manifest)
            done += 1
            print(f"✅  {len(combined):,} unikalnych id.")

        except Exception as e:
            print(f"❌  BŁĄD: {e}")

    print(f"\n✅ Resources: {done}/{total} plików przetworzonych.\n")


# ── PODSUMOWANIE ──────────────────────────────────────────────────────────────
def print_summary():
    print("=" * 60)
    print("PODSUMOWANIE MIGRACJI")
    print("=" * 60)

    if PLAYLOG_PARQUET.exists():
        df = pd.read_parquet(PLAYLOG_PARQUET)
        dates = sorted(df["DateEnd"].unique())
        size_mb = PLAYLOG_PARQUET.stat().st_size / 1e6
        print(f"\n📊 playlog_history.parquet")
        print(f"   Wierszy:    {len(df):,}")
        print(f"   Rozmiar:    {size_mb:.1f} MB")
        print(f"   Od:         {dates[0] if dates else '?'}")
        print(f"   Do:         {dates[-1] if dates else '?'}")
        print(f"   Unikalnych dat: {len(dates)}")
    else:
        print("\n⚠️  playlog_history.parquet — brak pliku!")

    if RESOURCES_PARQUET.exists():
        df = pd.read_parquet(RESOURCES_PARQUET)
        size_mb = RESOURCES_PARQUET.stat().st_size / 1e6
        print(f"\n📊 resources_latest.parquet")
        print(f"   Unikalnych id: {len(df):,}")
        print(f"   Rozmiar:       {size_mb:.1f} MB")
    else:
        print("\n⚠️  resources_latest.parquet — brak pliku!")

    print("\n" + "=" * 60)
    print("NASTĘPNE KROKI:")
    print("  1. Zweryfikuj powyższe liczby (czy daty się zgadzają?)")
    print("  2. Jeśli OK → usuń .txt:")
    print("     rm /dane/Broadsign_Logs/playlog/*.txt")
    print("     rm /dane/Broadsign_Logs/resources/*.txt")
    print("  3. Wdróż ScriptLinux_v4.py jako nowy ScriptLinux.py")
    print("=" * 60)


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    resume = "--resume" not in sys.argv  # domyślnie resume=True (pomija przetworzone)
    force  = "--force"  in sys.argv      # --force: przepisz wszystko od nowa

    if force:
        resume = False
        # usuń istniejące pliki wynikowe
        for p in [PLAYLOG_PARQUET, RESOURCES_PARQUET, MANIFEST_FILE]:
            if p.exists():
                p.unlink()
                print(f"🗑️  Usunięto {p.name}")

    print(f"\n{'='*60}")
    print(f"MIGRACJA DO PARQUET  —  {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    print(f"Tryb: {'od nowa (--force)' if force else 'resume (pomija przetworzone)'}")
    print(f"{'='*60}\n")

    manifest = load_manifest()

    migrate_playlogs(manifest, resume=resume)
    migrate_resources(manifest, resume=resume)
    print_summary()
