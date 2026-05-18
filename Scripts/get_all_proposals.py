import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from Package.auth import get_session
from Package.proposals import get_all_proposals

OUTPUT = Path(__file__).resolve().parent.parent / "Data" / "all_proposals.xlsx"

COLUMNS = [
    "id", "name", "status", "advertiser", "client_name",
    "start_date", "end_date", "price", "discount",
    "contract_id", "owner_user_id", "creation_tm", "modification_tm",
]


if __name__ == "__main__":
    session = get_session()
    proposals = get_all_proposals(session)

    df = pd.DataFrame(proposals)
    df = df[[c for c in COLUMNS if c in df.columns]]

    df.to_excel(OUTPUT, index=False)
    print(f"\nZapisano {len(df)} kampanii do {OUTPUT}")
