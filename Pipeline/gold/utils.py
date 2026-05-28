"""
Gold layer utilities.
Wejście: Data/silver/*.parquet + Data/bronze/*.parquet
Wyjście: Data/gold/*.parquet  (star schema gotowy do Power BI)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from datetime import datetime

BRONZE_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "bronze"
SILVER_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "silver"
GOLD_DIR   = Path(__file__).resolve().parent.parent.parent / "Data" / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)

# Kampanie wykluczone z raportowania budżetowego i play_logs
EXCLUDED_CAMPAIGN_IDS = {
    2617443,  # autopromocja
    2223525,  # czas dla metra (niekomercyjne komunikaty, umowa z metro)
}


def read_bronze(name: str) -> pd.DataFrame:
    return pd.read_parquet(BRONZE_DIR / f"{name}.parquet")


def read_silver(name: str) -> pd.DataFrame:
    return pd.read_parquet(SILVER_DIR / f"{name}.parquet")


def save_gold(df: pd.DataFrame, name: str) -> Path:
    """Zawsze pełny overwrite."""
    df = df.copy()
    df["_gold_at"] = datetime.utcnow().isoformat()
    path = GOLD_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"  -> {path.name}: {len(df)} wierszy, {len(df.columns)} kolumn")
    return path
