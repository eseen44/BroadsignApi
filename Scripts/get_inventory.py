import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from Package.auth import get_session
from Package.inventory import get_all_screens, get_screens_frames_mapping

OUTPUT_SCREENS  = Path(__file__).resolve().parent.parent / "Data" / "inventory_screens.xlsx"
OUTPUT_MAPPING  = Path(__file__).resolve().parent.parent / "Data" / "screens_frames_mapping.xlsx"


if __name__ == "__main__":
    session = get_session()

    # Ekrany digital
    screens = get_all_screens(session, inventory_type="digital")
    df_screens = pd.DataFrame(screens)
    df_screens.to_excel(OUTPUT_SCREENS, index=False)
    print(f"Zapisano {len(df_screens)} ekranów -> {OUTPUT_SCREENS}")

    # Mapping Direct ↔ Control ↔ Platform
    mapping = get_screens_frames_mapping(session)
    df_mapping = pd.DataFrame(mapping)
    print(f"\nMapping: {len(df_mapping)} rekordów, kolumny: {df_mapping.columns.tolist()}")
    df_mapping.to_excel(OUTPUT_MAPPING, index=False)
    print(f"Zapisano -> {OUTPUT_MAPPING}")
