"""
Narzędzia wspólne dla bronze pipeline.

Trzy strategie odświeżania:
  save_parquet()   — pełny overwrite (master data: proposals, screens, ctrl_display_units...)
  upsert_parquet() — upsert po kluczu id (incremental: ctrl_reservations, ctrl_content...)
  append_parquet() — append nowych dat (time-series: play_logs)

Kursory (not_modified_since) dla Control API trzymane w Data/bronze/_cursors.json.
"""
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

BRONZE_DIR = Path(__file__).resolve().parent.parent.parent / "Data" / "bronze"
CURSORS_FILE = BRONZE_DIR / "_cursors.json"


# ---------------------------------------------------------------------------
# Cursor management (Control API not_modified_since)
# ---------------------------------------------------------------------------

def load_cursors() -> dict:
    if CURSORS_FILE.exists():
        return json.loads(CURSORS_FILE.read_text(encoding="utf-8"))
    return {}


def get_cursor(key: str, default: str = "1970-01-01T00:00:00.") -> str:
    return load_cursors().get(key, default)


def save_cursor(key: str, value: str) -> None:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    cursors = load_cursors()
    cursors[key] = value
    CURSORS_FILE.write_text(json.dumps(cursors, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def stringify_nested(df: pd.DataFrame) -> pd.DataFrame:
    """Kolumny z listami / słownikami → JSON string (parquet nie lubi mixed types)."""
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna()
            if len(sample) > 0 and isinstance(sample.iloc[0], (list, dict)):
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
                )
    return df


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Strategia 1: pełny overwrite
# ---------------------------------------------------------------------------

def save_parquet(df: pd.DataFrame, name: str) -> Path:
    """
    Zastępuje cały plik nową zawartością.
    Używaj dla: proposals, proposal_items, screens, users, ctrl_display_units,
                ctrl_players, ctrl_customers, ctrl_users, fill_rate.
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["_fetched_at"] = _now()
    df = stringify_nested(df)
    path = BRONZE_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    print(f"  -> [overwrite] {len(df)} wierszy -> {path.name}")
    return path


# ---------------------------------------------------------------------------
# Strategia 2: upsert po id (incremental Control API)
# ---------------------------------------------------------------------------

def upsert_parquet(df: pd.DataFrame, name: str, key_col: str = "id") -> Path:
    """
    Nowe rekordy dodaje, zmienione (po key_col) nadpisuje, niezmienione zostawia.
    Używaj dla: ctrl_reservations, ctrl_bundles, ctrl_bundle_content,
                ctrl_schedules, ctrl_content.
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    path = BRONZE_DIR / f"{name}.parquet"

    df = df.copy()
    df["_fetched_at"] = _now()
    df = stringify_nested(df)

    # Deduplikuj wejsciowy df po kluczu (API czasem zwraca duplikaty w jednym fetchu)
    before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="last")
    if len(df) < before:
        print(f"  -> [upsert] usunieto {before - len(df)} duplikatow w df wejsciowym (po {key_col})")

    if len(df) == 0:
        # Nic do upsertowania — nie dotykaj istniejącego pliku
        existing_count = len(pd.read_parquet(path)) if path.exists() else 0
        print(f"  -> [upsert] brak nowych rekordow -> {path.name} bez zmian ({existing_count} wierszy)")
        return path

    if path.exists():
        existing = pd.read_parquet(path)
        # Usuń stare wersje rekordów które teraz aktualizujemy
        keep = existing[~existing[key_col].isin(df[key_col])]
        merged = pd.concat([keep, df], ignore_index=True)
        new_count = len(df)
        total = len(merged)
        # Napraw niezgodnosci typow: jezeli kolumna ma mieszane typy -> str
        for col in merged.columns:
            if merged[col].dtype == object:
                try:
                    merged[col] = merged[col].astype(str).where(merged[col].notna(), other=None)
                except Exception:
                    pass
    else:
        merged = df
        new_count = len(df)
        total = len(df)

    merged.to_parquet(path, index=False, engine="pyarrow")
    print(f"  -> [upsert] +{new_count} rekordow -> lacznie {total} w {path.name}")
    return path


# ---------------------------------------------------------------------------
# Strategia 3: append nowych dat (play_logs) — legacy, używane przez import_historical
# ---------------------------------------------------------------------------

def append_parquet(df: pd.DataFrame, name: str, date_col: str = "DateEnd") -> tuple:
    """
    Dorzuca wiersze których data (date_col) nie istnieje jeszcze w parquecie.
    Używaj dla: import_historical (bootstrap z CSV).
    Dla codziennego fetch z popstats używaj upsert_by_date().
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    path = BRONZE_DIR / f"{name}.parquet"

    if path.exists():
        existing = pd.read_parquet(path)
        existing_dates = set(existing[date_col].astype(str).unique())
        new_rows = df[~df[date_col].astype(str).isin(existing_dates)].copy()

        if len(new_rows) == 0:
            print(f"  -> [append] brak nowych dat -> {path.name} bez zmian")
            return path, 0

        new_rows["_fetched_at"] = _now()
        new_rows = stringify_nested(new_rows)
        merged = pd.concat([existing, new_rows], ignore_index=True)
    else:
        new_rows = df.copy()
        new_rows["_fetched_at"] = _now()
        new_rows = stringify_nested(new_rows)
        merged = new_rows

    merged.to_parquet(path, index=False, engine="pyarrow")
    print(f"  -> [append] +{len(new_rows)} wierszy -> lacznie {len(merged)} w {path.name}")
    return path, len(new_rows)


# ---------------------------------------------------------------------------
# Strategia 4: upsert po dacie (fetch z popstats — jedyne źródło prawdy)
# ---------------------------------------------------------------------------

def upsert_by_date(df: pd.DataFrame, name: str, date_col: str = "DateEnd", date: str = None) -> tuple:
    """
    Zastępuje wszystkie wiersze dla podanej daty nowymi danymi z popstats.
    Używaj dla: fetch_incremental (codzienne pobieranie playlogów).

    Gwarantuje że play_logs.parquet odzwierciedla dokładnie dane z URL —
    bez domieszki danych z innych źródeł (playlog_history, CSV).
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    path = BRONZE_DIR / f"{name}.parquet"

    df = df.copy()
    df["_fetched_at"] = _now()
    df = stringify_nested(df)

    if path.exists():
        existing = pd.read_parquet(path)
        keep = existing[existing[date_col].astype(str) != str(date)]
        merged = pd.concat([keep, df], ignore_index=True)
    else:
        merged = df

    merged.to_parquet(path, index=False, engine="pyarrow")
    print(f"  -> [upsert_date] {date}: {len(df)} wierszy -> lacznie {len(merged)} w {path.name}")
    return path, len(df)
