"""
Silver: screen_bridge — mostek Direct API ↔ Control API

Buduje tablicę (FrameID, DisplayUnitID) z play_logs.
Ta para jest jedynym łącznikiem między:
  - Direct API (screens, screens_frames_mapping przez frame_id)
  - Control API (ctrl_display_units przez display_unit_id)

Wynikowy stół umożliwia:
  play_logs.FrameID → screen_bridge → screen_id → screen_name (Direct API)
  play_logs.DisplayUnitID → screen_bridge → display_unit_name (Control API)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.silver.utils import read_bronze, save_silver


def build_screen_bridge(sample_size: int = 0) -> pd.DataFrame:
    """
    sample_size: jeśli > 0, pobiera tylko tyle wierszy z play_logs
                 (dla testów, 0 = wszystkie).
    """
    print("  Wczytuję play_logs...")
    play_logs = read_bronze("play_logs")
    if sample_size > 0:
        play_logs = play_logs.sample(n=min(sample_size, len(play_logs)), random_state=42)

    screens_full = read_bronze("screens_frames_mapping")
    du = read_bronze("ctrl_display_units")
    players = read_bronze("ctrl_players")

    # Buduj unikalne pary (FrameID, DisplayUnitID) z play_logs
    # Filtruj zera (house ads / filler)
    mask = (
        play_logs["FrameID"].notna() &
        (play_logs["FrameID"] != 0) &
        play_logs["DisplayUnitID"].notna() &
        (play_logs["DisplayUnitID"] != 0)
    )
    pairs = play_logs[mask][["FrameID", "DisplayUnitID", "PlayerID"]].copy()
    pairs["FrameID"]       = pairs["FrameID"].astype("Int64")
    pairs["DisplayUnitID"] = pairs["DisplayUnitID"].astype("Int64")
    pairs["PlayerID"]      = pairs["PlayerID"].astype("Int64")

    # Agreguj: ile razy dana para wystąpiła, zbierz unikalne PlayerID
    bridge = (
        pairs.groupby(["FrameID", "DisplayUnitID"], dropna=True)
        .agg(
            play_count=("PlayerID", "count"),
            player_ids=("PlayerID", lambda x: sorted(x.dropna().unique().tolist())),
        )
        .reset_index()
    )

    # Dołącz informacje o ekranie (Direct API)
    # screens_frames_mapping: group_id = screens.id (stacja), screen_id = panel ID
    sfm = screens_full[["frame_id", "group_id"]].drop_duplicates()
    sfm["frame_id"]  = sfm["frame_id"].astype("Int64")
    sfm["group_id"]  = pd.to_numeric(sfm["group_id"], errors="coerce").astype("Int64")
    bridge = bridge.merge(
        sfm.rename(columns={"frame_id": "FrameID", "group_id": "screen_id"}),
        on="FrameID", how="left"
    )

    # Dołącz nazwę ekranu ze screens
    screens = read_bronze("screens")[["id", "name", "address"]].rename(columns={
        "id": "screen_id", "name": "screen_name", "address": "screen_address"
    })
    screens["screen_id"] = screens["screen_id"].astype("Int64")
    bridge = bridge.merge(screens, on="screen_id", how="left")

    # Dołącz informacje o display_unit (Control API)
    du_slim = du[["id", "name", "address"]].rename(columns={
        "id": "display_unit_id", "name": "display_unit_name", "address": "du_address"
    })
    du_slim["display_unit_id"] = du_slim["display_unit_id"].astype("Int64")
    bridge = bridge.merge(
        du_slim.rename(columns={"display_unit_id": "DisplayUnitID"}),
        on="DisplayUnitID", how="left"
    )

    # Dołącz player info
    pl_slim = players[["claimed_resource_id", "id", "name", "hostname"]].rename(columns={
        "claimed_resource_id": "display_unit_id_player",
        "id": "player_id",
        "name": "player_name",
        "hostname": "player_hostname",
    })

    # Upewnij się że player_ids to lista stringów / intów dla parquet
    bridge["player_ids"] = bridge["player_ids"].apply(
        lambda lst: ",".join(str(x) for x in lst) if isinstance(lst, list) else ""
    )

    bridge = bridge[[c for c in bridge.columns if c != "_fetched_at"]]

    save_silver(bridge, "screen_bridge")
    return bridge


if __name__ == "__main__":
    df = build_screen_bridge()
    print(df[["FrameID", "DisplayUnitID", "screen_name", "display_unit_name", "play_count"]].head(10).to_string())
    matched = df["screen_name"].notna().sum()
    total = len(df)
    print(f"\nUnique pairs: {total}, matched screen: {matched}/{total}")
