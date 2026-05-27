"""
Narzędzia wspólne dla silver pipeline.

Silver = zawsze pełny overwrite (obliczana na nowo z bronze przy każdym uruchomieniu).
"""
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

BRONZE_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "bronze"
SILVER_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "silver"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_bronze(name: str) -> pd.DataFrame:
    """Wczytuje plik z bronze layer."""
    path = BRONZE_DIR / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Bronze file not found: {path}")
    return pd.read_parquet(path)


def save_silver(df: pd.DataFrame, name: str) -> Path:
    """Zapisuje silver table (zawsze pełny overwrite)."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["_silver_at"] = _now()
    path = SILVER_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    print(f"  -> [silver] {len(df)} wierszy -> {path.name}")
    return path


def unpack_json_col(df: pd.DataFrame, col: str, prefix: str = "") -> pd.DataFrame:
    """
    Rozpakowuje kolumnę z JSON-string do osobnych kolumn.
    Bezpieczne — jeśli parsowanie się nie uda, pomija.
    """
    if col not in df.columns:
        return df

    def safe_parse(x):
        if pd.isnull(x):
            return {}
        if isinstance(x, dict):
            return x
        try:
            return json.loads(x)
        except Exception:
            return {}

    parsed = df[col].apply(safe_parse)
    if parsed.apply(lambda x: isinstance(x, dict) and len(x) == 0).all():
        return df  # Nic do rozpakowania

    expanded = pd.json_normalize(parsed)
    if prefix:
        expanded.columns = [f"{prefix}_{c}" for c in expanded.columns]

    expanded.index = df.index
    df = pd.concat([df.drop(columns=[col]), expanded], axis=1)
    return df
