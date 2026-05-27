"""
Narzędzia wspólne dla bronze pipeline.
"""
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

BRONZE_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "bronze"


def stringify_nested(df: pd.DataFrame) -> pd.DataFrame:
    """
    Kolumny zawierające listy lub słowniki zamień na JSON string.
    Parquet nie obsługuje mieszanych typów — bezpieczniej trzymać jako tekst w bronze.
    Silver layer może je sparsować.
    """
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if len(sample) > 0 and isinstance(sample.iloc[0], (list, dict)):
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
                )
    return df


def save_parquet(df: pd.DataFrame, name: str) -> Path:
    """
    Zapisuje DataFrame do Data/bronze/{name}.parquet.
    Dodaje kolumnę _fetched_at z timestampem UTC.
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["_fetched_at"] = datetime.now(timezone.utc).isoformat()
    df = stringify_nested(df)

    path = BRONZE_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    print(f"  -> Zapisano {len(df)} wierszy, {len(df.columns)} kolumn: {path.name}")
    return path
