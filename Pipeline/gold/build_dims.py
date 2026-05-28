"""
Gold — budowanie wszystkich wymiarów (dim_*).

dim_date       — kalendarz
dim_campaign   — kampania (proposal) + lineitem (proposal_item)
dim_screen     — ekran + frame
dim_player     — player + display_unit
dim_content    — treść/kreacja
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import pandas as pd
from Pipeline.gold.utils import read_bronze, read_silver, save_gold


# ---------------------------------------------------------------------------
# dim_date
# ---------------------------------------------------------------------------

def build_dim_date():
    """
    Generuje kalendarz obejmujący zakres danych z play_logs + kampanii.
    Zakres: 2024-01-01 → dziś + 365 dni (obsługa przyszłych kampanii).
    """
    from datetime import date, timedelta

    start = date(2024, 1, 1)
    end   = date.today() + timedelta(days=365)
    dates = pd.date_range(start=start, end=end, freq="D")

    df = pd.DataFrame({"date": dates})
    df["date_str"]     = df["date"].dt.strftime("%Y-%m-%d")   # klucz do joinów
    df["year"]         = df["date"].dt.year
    df["quarter"]      = df["date"].dt.quarter
    df["month"]        = df["date"].dt.month
    df["month_name"]   = df["date"].dt.strftime("%B")
    df["week"]         = df["date"].dt.isocalendar().week.astype(int)
    df["day_of_week"]  = df["date"].dt.dayofweek + 1          # 1=Pn, 7=Nd
    df["day_name"]     = df["date"].dt.strftime("%A")
    df["is_weekend"]   = df["day_of_week"].isin([6, 7])
    df["date"]         = df["date"].dt.date.astype(str)        # zostaw jako str

    save_gold(df.rename(columns={"date": "date_key"}), "dim_date")


# ---------------------------------------------------------------------------
# dim_campaign
# ---------------------------------------------------------------------------

def build_dim_campaign():
    """
    Z silver.campaigns (proposal_items × proposals × users).
    Klucz: line_item_id — to jest FK w fact_play_logs (przez reservation) i fact_budget.
    """
    df = read_silver("campaigns")

    keep = [
        "line_item_id", "campaign_id",
        "line_item_name", "campaign_name",
        "advertiser", "client_id", "client_name",
        "type_of_buy", "slot_duration", "screen_count", "group_count",
        "status_id", "status_name",
        "line_price", "line_suggested_price",
        "campaign_price", "campaign_suggested_price", "campaign_discount",
        "buy_saturation", "buy_bs_saturation", "buy_sov", "buy_budget",
        "perf_expected_repetitions", "perf_actual_repetitions",
        "perf_expected_impressions", "perf_actual_impressions",
        "line_start", "line_end",
        "campaign_start", "campaign_end",
        "owner_email", "owner_user_id",
        "contract_id", "contract_number",
        "is_preemptible",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    df = df.drop_duplicates(subset=["line_item_id"])

    # Liczba dni trwania lineitemu (do podziału kosztu)
    df["line_start"] = pd.to_datetime(df["line_start"], errors="coerce").dt.date.astype(str)
    df["line_end"]   = pd.to_datetime(df["line_end"],   errors="coerce").dt.date.astype(str)
    df["line_days"]  = (
        pd.to_datetime(df["line_end"]) - pd.to_datetime(df["line_start"])
    ).dt.days + 1
    df["line_days"]  = df["line_days"].clip(lower=1)

    # campaign_price alokowany do lineitemu proporcjonalnie do line_price
    # sum line_price per campaign
    camp_sum = df.groupby("campaign_id")["line_price"].transform("sum")
    camp_sum = camp_sum.replace(0, None)
    df["campaign_price_share"] = df["line_price"] / camp_sum
    df["campaign_price_allocated"] = df["campaign_price"] * df["campaign_price_share"]

    save_gold(df, "dim_campaign")


# ---------------------------------------------------------------------------
# dim_screen
# ---------------------------------------------------------------------------

def build_dim_screen():
    """
    Z silver.screens_full (screens × screens_frames_mapping).
    Klucz: frame_id — to jest FrameID w play_logs.
    """
    df = read_silver("screens_full")

    keep = [
        "frame_id", "screen_id",
        "screen_name", "screen_address",
        "panel_id", "panel_uuid", "screen_uuid",
        "resolution", "orientation",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    df = df.drop_duplicates(subset=["frame_id"])

    # Konwersja typów do string dla Power BI
    for col in ["frame_id", "screen_id", "panel_id"]:
        if col in df.columns:
            df[col] = df[col].astype("Int64")

    save_gold(df, "dim_screen")


# ---------------------------------------------------------------------------
# dim_player
# ---------------------------------------------------------------------------

def build_dim_player():
    """
    Z silver.players_full (ctrl_players × ctrl_display_units).
    Klucz: play_log_player_id — to jest PlayerID w play_logs.
    """
    df = read_silver("players_full")

    keep = [
        "play_log_player_id", "player_id",
        "player_name", "hostname",
        "display_unit_id", "display_unit_name", "du_address",
        "timezone", "nscreens",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    df = df.drop_duplicates(subset=["play_log_player_id"])

    save_gold(df, "dim_player")


# ---------------------------------------------------------------------------
# dim_content
# ---------------------------------------------------------------------------

def build_dim_content():
    """
    Łączy ctrl_content z resources_latest (popstats).
    Klucz: content_id — to jest AdCopyId w play_logs.
    """
    content = read_bronze("ctrl_content")[["id", "name", "domain_id"]].copy()
    content = content.rename(columns={"id": "content_id", "name": "content_name"})
    content["content_id"] = pd.to_numeric(content["content_id"], errors="coerce").astype("Int64")

    # resources_latest — typ 'content' z popstats
    try:
        res = read_bronze("resources_latest")
        res_content = res[res["type"] == "content"][["id", "name"]].copy()
        res_content = res_content.rename(columns={"id": "content_id", "name": "popstats_name"})
        res_content["content_id"] = pd.to_numeric(res_content["content_id"], errors="coerce").astype("Int64")
        content = content.merge(res_content, on="content_id", how="left")
    except Exception:
        content["popstats_name"] = None

    # Preferuj nazwę z Control API, fallback na popstats
    content["content_name"] = content["content_name"].fillna(content.get("popstats_name", None))
    content = content.drop(columns=["popstats_name"], errors="ignore")
    content = content.drop_duplicates(subset=["content_id"])

    save_gold(content, "dim_content")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def build_all_dims():
    print("[dim_date]")
    build_dim_date()
    print("[dim_campaign]")
    build_dim_campaign()
    print("[dim_screen]")
    build_dim_screen()
    print("[dim_player]")
    build_dim_player()
    print("[dim_content]")
    build_dim_content()


if __name__ == "__main__":
    build_all_dims()
