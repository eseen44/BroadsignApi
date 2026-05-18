import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from Package.auth import get_session

OUTPUT = Path(__file__).resolve().parent.parent / "Data" / "current_user.xlsx"


def get_current_user(session):
    resp = session.get("https://direct.broadsign.com/api/v1/user/current_user")
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    user = resp.json()["user"]
    user_info = pd.DataFrame([{k: v for k, v in user.items() if not isinstance(v, (list, dict))}])
    privileges = pd.DataFrame(user.get("privileges", []), columns=["privilege"])
    features = pd.DataFrame(user.get("feature_flags", []), columns=["feature_flag"])
    return user_info, privileges, features


if __name__ == "__main__":
    session = get_session()
    user_info, privileges, features = get_current_user(session)

    print("=== User Info ===")
    print(user_info.T)
    print("\n=== Privileges ===")
    print(privileges)
    print("\n=== Feature Flags ===")
    print(features)

    with pd.ExcelWriter(OUTPUT) as writer:
        user_info.to_excel(writer, sheet_name="user_info", index=False)
        privileges.to_excel(writer, sheet_name="privileges", index=False)
        features.to_excel(writer, sheet_name="feature_flags", index=False)
    print(f"\nZapisano do {OUTPUT}")
