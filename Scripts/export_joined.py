import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from Package.auth import get_session
from Package.reporting import get_all_fill_rate
from Package.proposals import get_all_proposals
from Package.proposal_items import get_all_proposal_items
from Package.inventory import get_all_screens, get_screens_frames_mapping

OUTPUT_CAMPAIGNS = Path(__file__).resolve().parent.parent / "Data" / "joined_campaigns.csv"
OUTPUT_SCREENS   = Path(__file__).resolve().parent.parent / "Data" / "joined_screens.csv"


def flatten_col(df, col, prefix=None):
    """Rozwin kolumne ze slownikami (dict) na osobne kolumny."""
    if col not in df.columns:
        return df
    expanded = df[col].apply(lambda x: x if isinstance(x, dict) else {})
    expanded = pd.json_normalize(expanded)
    if prefix:
        expanded.columns = [f"{prefix}_{c}" for c in expanded.columns]
    df = df.drop(columns=[col])
    df = pd.concat([df, expanded], axis=1)
    return df


if __name__ == "__main__":
    session = get_session()

    # --- 1. Inventory (screen_ids do fill rate) ---
    print("Pobieranie ekranow...")
    screens = get_all_screens(session, inventory_type="digital")
    screen_ids = [s["id"] for s in screens]
    print(f"  Ekranow: {len(screen_ids)}")

    # --- 2. Fill rate (ostatnie 7 dni, wszystkie ekrany) ---
    print("Pobieranie fill rate...")
    fill_rate = get_all_fill_rate(session, screen_ids)
    df_fr = pd.DataFrame(fill_rate)
    df_fr = flatten_col(df_fr, "priority", prefix="priority")
    df_fr = flatten_col(df_fr, "buy_details", prefix="buy")
    print(f"  Fill rate rekordow: {len(df_fr)}")

    # --- 3. Proposal items ---
    print("Pobieranie proposal items...")
    proposal_items = get_all_proposal_items(session)
    df_pi = pd.DataFrame(proposal_items)
    df_pi = flatten_col(df_pi, "priority", prefix="pi_priority")
    df_pi = flatten_col(df_pi, "buy_details", prefix="pi_buy")
    df_pi = flatten_col(df_pi, "performance", prefix="perf")
    # Zostaw tylko kolumny przydatne do joina
    pi_cols = ["id", "status_name", "slot_duration", "screen_count", "group_count",
               "buyer", "perf_expected_repetitions", "perf_actual_repetitions",
               "perf_projected_repetitions", "perf_status"]
    pi_cols = [c for c in pi_cols if c in df_pi.columns]
    df_pi_slim = df_pi[pi_cols].rename(columns={"id": "line_id"})
    print(f"  Proposal items: {len(df_pi_slim)}")

    # --- 4. Proposals ---
    print("Pobieranie proposals (kampanie)...")
    proposals = get_all_proposals(session)
    df_p = pd.DataFrame(proposals)
    # Zostaw tylko kolumny przydatne do joina
    p_cols = ["id", "contract_number", "contact_name", "contact_phone",
              "contact_email", "discount", "owner_user_name", "category_ids"]
    p_cols = [c for c in p_cols if c in df_p.columns]
    df_p_slim = df_p[p_cols].rename(columns={"id": "campaign_id"})
    # category_ids to lista - zamien na string
    if "category_ids" in df_p_slim.columns:
        df_p_slim["category_ids"] = df_p_slim["category_ids"].apply(
            lambda x: ",".join(str(i) for i in x) if isinstance(x, list) else x
        )
    print(f"  Proposals: {len(df_p_slim)}")

    # --- 5. JOIN: fill_rate + proposal_items + proposals ---
    print("Laczenie danych...")
    df = df_fr.merge(df_pi_slim, on="line_id", how="left")
    df = df.merge(df_p_slim, on="campaign_id", how="left")

    # Porzadek kolumn
    first_cols = ["campaign_id", "campaign_name", "advertiser", "client",
                  "line_id", "line_name", "start_date", "end_date",
                  "status", "status_name", "type_of_buy", "price",
                  "fill_pressure", "buy_saturation", "buy_bs_saturation",
                  "screens_number", "screen_count",
                  "perf_expected_repetitions", "perf_actual_repetitions",
                  "perf_status", "priority_level", "priority_name",
                  "owner_name", "owner_user_name",
                  "contract_number", "contact_name", "contact_email",
                  "discount", "slot_duration", "buyer"]
    # Tylko te ktore istnieja
    first_cols = [c for c in first_cols if c in df.columns]
    rest_cols  = [c for c in df.columns if c not in first_cols]
    df = df[first_cols + rest_cols]

    df.to_csv(OUTPUT_CAMPAIGNS, index=False, encoding="utf-8-sig")
    print(f"\nZapisano {len(df)} rekordow -> {OUTPUT_CAMPAIGNS}")
    print(df.dtypes)
    print(df.head(3).to_string())

    # --- 6. Screens + mapping ---
    print("\nPobieranie screens frames mapping...")
    mapping = get_screens_frames_mapping(session)
    df_screens   = pd.DataFrame(screens)
    df_mapping   = pd.DataFrame(mapping)

    df_mapping["screen_id"] = pd.to_numeric(df_mapping["screen_id"], errors="coerce")
    df_screens_full = df_screens.merge(
        df_mapping[["screen_id", "frame_id", "screen_uuid", "group_id", "group_uuid"]],
        left_on="id", right_on="screen_id", how="left"
    ).drop(columns=["screen_id"])

    df_screens_full.to_csv(OUTPUT_SCREENS, index=False, encoding="utf-8-sig")
    print(f"Zapisano {len(df_screens_full)} rekordow -> {OUTPUT_SCREENS}")
