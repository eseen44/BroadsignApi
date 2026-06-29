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
# Kampanie serwisowe / niekomercyjne — is_serwisowy = 1 w dim_campaign i fact_play_logs
SERWISOWY_CAMPAIGN_IDS = {
    2223525,   # Copy_of_!!Czas dla Metra
    3238917,   # Czas dla Metra DMB
    3379967,   # Test Parking lotnisko
    3072428,   # Lotnisko na mieście
    2312735,   # Stroer Promo TP
    2236243,   # Stroer Promo DMB
    2654184,   # Roadside_Promo (Stroer)
    3324704,   # Roadside_Promo (Stroer Polska)
    2617443,   # Autopromocja Liveline
    2534697,   # OMD autopromocja
    2288610,   # Kr/Wr autopromocja
    2108200,   # TrainArrivingMessage (Direct API)
    3005804,   # Gromada
    2464079,   # Miasto30
    3043940,   # Parking przy lotnisku
}

# Rezerwacje serwisowe po reservation_id
SERWISOWY_RESERVATION_IDS = {
    1125116202,  # TrainArrivingMessage — systemowy komunikat
    1331907375,  # Promo Road mk2
    1243202200,  # Roadside_Promo
    1230744166,  # test_IT_synchro
}

# Zachowane dla kompatybilności wstecznej (puste — nic nie wykluczamy twardo)
EXCLUDED_CAMPAIGN_IDS: set = set()
EXCLUDED_RESERVATION_IDS: set = set()


def get_single_panel_campaign_ids() -> set:
    """
    Zwraca zbiór campaign_id które mają dokładnie 1 unikalny panel w play logach
    i nie są już w SERWISOWY_CAMPAIGN_IDS (is_serwisowy=1).
    Używane do oznaczenia is_serwisowy=2 (kampanie testowe/diagnostyczne).
    """
    pl = pd.read_parquet(BRONZE_DIR / "play_logs.parquet",
                         columns=["CampID", "PlayerID"]).dropna()
    pl["CampID"]   = pd.to_numeric(pl["CampID"],   errors="coerce").astype("Int64")
    pl["PlayerID"] = pd.to_numeric(pl["PlayerID"], errors="coerce").astype("Int64")
    pl = pl.dropna()

    # CampID (reservation) → campaign_id przez ctrl_reservations_v22 + silver campaigns
    res22 = pd.read_parquet(BRONZE_DIR / "ctrl_reservations_v22.parquet",
                            columns=["id", "proposal_line_item_id"])
    res22["id"]                   = pd.to_numeric(res22["id"],                   errors="coerce").astype("Int64")
    res22["proposal_line_item_id"] = pd.to_numeric(res22["proposal_line_item_id"], errors="coerce").astype("Int64")
    res22 = res22.dropna()

    camps = pd.read_parquet(SILVER_DIR / "campaigns.parquet",
                            columns=["line_item_id", "campaign_id"]).drop_duplicates("line_item_id")
    camps["line_item_id"] = pd.to_numeric(camps["line_item_id"], errors="coerce").astype("Int64")
    camps["campaign_id"]  = pd.to_numeric(camps["campaign_id"],  errors="coerce").astype("Int64")

    res22 = res22.merge(camps.rename(columns={"line_item_id": "proposal_line_item_id"}),
                        on="proposal_line_item_id", how="left")

    pl = pl.merge(res22.rename(columns={"id": "CampID"})[["CampID", "campaign_id"]],
                  on="CampID", how="left")

    panels = (pl.dropna(subset=["campaign_id"])
               .groupby("campaign_id")["PlayerID"]
               .nunique())

    single = set(panels[panels == 1].index.astype(int))
    return single - SERWISOWY_CAMPAIGN_IDS


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
