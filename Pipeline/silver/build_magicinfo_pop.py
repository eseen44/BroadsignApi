"""
Silver: magicinfo_pop

Czysci dane Bronze MagicInfo PoP:
  - Usuwa rekordy z format="other" (content bez rozdzielczosci w CMS, <1% emisji)
  - Usuwa wiersze z play_count=0
  - Normalizuje play_date do daty

Granulacja wyjsciowa: format x content x dzien (jedna emisja per kombinacja).
Dane bez joina z Broadsign -- to zadanie Gold.

Wyjscie: Data/silver/magicinfo_pop.parquet
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.silver.utils import read_bronze, save_silver


def build_magicinfo_pop() -> pd.DataFrame:
    print("  Wczytuje magicinfo_pop z Bronze...")
    df = read_bronze("magicinfo_pop")
    print(f"  Bronze: {len(df)} wierszy, formaty: {df['format'].value_counts().to_dict()}")

    # Filtruj: tylko znane formaty i pozytywne emisje
    df = df[df["format"].isin(["liveline", "stroertv", "triplay"])].copy()
    df = df[df["play_count"] > 0].copy()
    print(f"  Po filtracji: {len(df)} wierszy")

    # Normalizuj typy
    df["play_date"]     = pd.to_datetime(df["play_date"], errors="coerce").dt.date
    df["play_count"]    = pd.to_numeric(df["play_count"],    errors="coerce").fillna(0).astype(int)
    df["duration_secs"] = pd.to_numeric(df["duration_secs"], errors="coerce").fillna(0).astype(int)

    # Zachowane kolumny
    cols = ["format", "content_id", "content_name", "content_res",
            "play_date", "play_count", "duration_secs"]
    df = df[cols].sort_values(["format", "play_date", "content_name"]).reset_index(drop=True)

    # Podsumowanie
    for fmt, grp in df.groupby("format"):
        print(f"    {fmt}: {len(grp)} wierszy, "
              f"{grp['content_name'].nunique()} contentow, "
              f"{grp['play_count'].sum():,} emisji, "
              f"{round(grp['duration_secs'].sum()/3600)} h")

    return save_silver(df, "magicinfo_pop")


if __name__ == "__main__":
    build_magicinfo_pop()
