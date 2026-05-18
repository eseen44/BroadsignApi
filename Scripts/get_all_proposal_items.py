import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from Package.auth import get_session
from Package.proposal_items import get_all_proposal_items

OUTPUT = Path(__file__).resolve().parent.parent / "Data" / "all_proposal_items.xlsx"

COLUMNS = [
    "id", "proposal_id", "name", "status",
    "start_date", "end_date", "start_time", "end_time",
    "price", "custom_price", "suggested_price",
    "actual_impressions", "actual_repetitions",
    "screen_count", "audience_count",
    "slot_duration", "saturation", "flight_duration",
    "provider_id", "external_id",
    "creation_tm", "modification_tm",
]


if __name__ == "__main__":
    session = get_session()
    items = get_all_proposal_items(session)

    df = pd.DataFrame(items)
    df = df[[c for c in COLUMNS if c in df.columns]]

    df.to_excel(OUTPUT, index=False)
    print(f"\nZapisano {len(df)} line itemów do {OUTPUT}")
