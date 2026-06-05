"""
Silver: play_logs_enriched

play_logs wzbogacone o:
  - Nazwa kampanii z ctrl_reservations (CampID = reservation.id)
  - Nazwa ekranu z screens (FrameID → screens_frames_mapping → screens.name)
  - Dane kampanii Direct API (contract_id → proposals.contract_number)
  - display_unit_name z ctrl_display_units (DisplayUnitID)

Filtrujemy: CampID=0 (house ads), DisplayUnitID=0 (filler).
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.silver.utils import read_bronze, save_silver


def build_play_logs(filter_zeros: bool = True) -> pd.DataFrame:
    print("  Wczytuję play_logs...")
    pl = read_bronze("play_logs")

    if filter_zeros:
        pl = pl[(pl["CampID"] != 0) & (pl["DisplayUnitID"] != 0)].copy()
        print(f"  Po filtracji zer: {len(pl)} wierszy")

    # Wymuś Int64 na ID-kolumnach
    for col in ["CampID", "DisplayUnitID", "AdCopyId", "FrameID", "PlayerID"]:
        if col in pl.columns:
            pl[col] = pd.to_numeric(pl[col], errors="coerce").astype("Int64")

    # ----------------------------------------------------------------
    # 1. Nazwa rezerwacji z ctrl_reservations (CampID = reservation.id)
    # ----------------------------------------------------------------
    reservations = read_bronze("ctrl_reservations")[["id", "name", "start_date", "end_date", "state"]].rename(columns={
        "id":         "CampID",
        "name":       "reservation_name",
        "start_date": "reservation_start",
        "end_date":   "reservation_end",
        "state":      "reservation_state",
    })
    reservations["CampID"] = reservations["CampID"].astype("Int64")
    pl = pl.merge(reservations, on="CampID", how="left")

    # ----------------------------------------------------------------
    # 2. Ekran z screens_frames_mapping + screens (FrameID → screen)
    # Uwaga: group_id w SFM = screens.id (stacja), screen_id = panel ID
    # ----------------------------------------------------------------
    sfm = read_bronze("screens_frames_mapping")[["frame_id", "group_id"]].drop_duplicates()
    sfm["frame_id"] = sfm["frame_id"].astype("Int64")
    sfm["group_id"] = pd.to_numeric(sfm["group_id"], errors="coerce").astype("Int64")

    screens = read_bronze("screens")[["id", "name", "address"]].rename(columns={
        "id": "screen_id", "name": "screen_name", "address": "screen_address"
    })
    screens["screen_id"] = screens["screen_id"].astype("Int64")

    sfm_screens = sfm.merge(
        screens.rename(columns={"screen_id": "group_id"}),
        on="group_id", how="left"
    ).rename(columns={"frame_id": "FrameID", "group_id": "screen_id"})

    pl = pl.merge(sfm_screens, on="FrameID", how="left")

    # ----------------------------------------------------------------
    # 3. Nazwa display_unit z ctrl_display_units (DisplayUnitID)
    # ----------------------------------------------------------------
    du = read_bronze("ctrl_display_units")[["id", "name", "address"]].rename(columns={
        "id": "DisplayUnitID", "name": "display_unit_name", "address": "du_address"
    })
    du["DisplayUnitID"] = du["DisplayUnitID"].astype("Int64")
    pl = pl.merge(du, on="DisplayUnitID", how="left")

    # Fallback: 42 frames ma null group_id w SFM, 37 nie ma w SFM w ogóle
    # → łącznie ~9.6% wierszy bez screen_name. display_unit_name (Control API) ma 100% pokrycia.
    pl["screen_name"] = pl["screen_name"].fillna(pl["display_unit_name"])

    # ----------------------------------------------------------------
    # 4. Kampania Direct API przez contract_id → proposals.contract_number
    # ----------------------------------------------------------------
    proposals = read_bronze("proposals")[[
        "id", "name", "advertiser", "client_name", "contract_number", "status",
        "start_date", "end_date"
    ]].rename(columns={
        "id":              "proposal_id",
        "name":            "proposal_name",
        "start_date":      "proposal_start",
        "end_date":        "proposal_end",
    })
    proposals_with_contract = proposals[proposals["contract_number"].notna()].copy()

    pl = pl.merge(
        proposals_with_contract.rename(columns={"contract_number": "contract_id"}),
        on="contract_id",
        how="left"
    )

    # ----------------------------------------------------------------
    # Sortuj i wybierz kolumny
    # ----------------------------------------------------------------
    output_cols = [
        # Identyfikatory
        "DateEnd", "timeslot", "timeslot_label",
        "PlayerID", "FrameID", "DisplayUnitID", "AdCopyId", "CampID", "contract_id",
        # Metryki
        "emisje", "Impresje", "Duration",
        # Nazwy — Control API
        "reservation_name", "reservation_state",
        "reservation_start", "reservation_end",
        "display_unit_name", "du_address",
        # Nazwy — Direct API
        "screen_id", "screen_name", "screen_address",
        "proposal_id", "proposal_name", "advertiser", "client_name",
        "proposal_start", "proposal_end", "status",
    ]
    output_cols = [c for c in output_cols if c in pl.columns]
    pl = pl[output_cols]

    pl["DateEnd"] = pd.to_datetime(pl["DateEnd"], errors="coerce")

    save_silver(pl, "play_logs_enriched")
    return pl


if __name__ == "__main__":
    df = build_play_logs()
    print(df[["DateEnd", "CampID", "reservation_name", "screen_name", "display_unit_name", "emisje"]].head(10).to_string())
    print(f"\nMatch reservation: {df['reservation_name'].notna().sum()}/{len(df)} ({df['reservation_name'].notna().mean()*100:.1f}%)")
    print(f"Match screen:      {df['screen_name'].notna().sum()}/{len(df)} ({df['screen_name'].notna().mean()*100:.1f}%)")
    print(f"Match proposal:    {df['proposal_name'].notna().sum()}/{len(df)} ({df['proposal_name'].notna().mean()*100:.1f}%)")
