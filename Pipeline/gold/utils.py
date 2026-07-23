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
    2984174,   # Promo Road mk2 -- byla juz w SERWISOWY_RESERVATION_IDS (reservation 1331907375),
               # ale brakowalo jej tu na poziomie campaign_id (niespojnosc dim_campaign vs
               # fact_play_logs, znaleziona i potwierdzona przez usera 2026-07-15)
    2617443,   # Autopromocja Liveline
    2486500,   # LL-autopromo- test
    2288610,   # Kr/Wr autopromocja
    2108200,   # TrainArrivingMessage (Direct API)
    3005804,   # Gromada
    2464079,   # Miasto30
    3043940,   # Parking przy lotnisku
    3706832,   # Promo TP (advertiser/client=Stroer_TP, 0 zl -- analog Stroer Promo TP)
    3706837,   # DMB_Promo (advertiser/client=Stroer_DMB, 0 zl -- analog Stroer Promo DMB)
    2986317,   # roadside promo niezainstalowane (brak advertisera, 0 zl)
    3105116,   # Promo Road Mroz (advertiser/client=Stroer, 104 287 zl)
    3071680,   # Promo_Stroer_Sw_Road (advertiser/client=Stroer, 304 730 zl)
    3609971,   # Citylight Digital Promo (advertiser/client=Stroer, 1 358 390 zl)
    3071824,   # Programatic_Promo (advertiser=Stroer, 1 660 435 zl)
    # ^ wszystkie 7 znalezione regexem "promo" + advertiser/client=Stroer,
    # potwierdzone przez usera 2026-07-16 (autopromo/wewnetrzne, nie realny
    # przychod od klienta mimo niezerowych cen)
    900000001,  # syntetyczny -- "Uwaga Pociąg", patrz MANUAL_RESERVATION_OVERRIDES
    900000002,  # syntetyczny -- "Autopromocja" (19 rezerwacji bez bookingu), patrz nizej
    # Programmatic "always-on" kontenery: cena 0, przychod 0, sentinelowa data konca
    # (2099/2078) -- puste kanaly programmatic, nie realne kampanie. Psuly statystyki
    # (czas trwania: 26 844 dni). Zero przychodu do ukrycia. User 2026-07-23.
    # (Programatic_Promo 3071824 z realnym 1,66 mln JUZ wyzej -- autopromo.)
    3705906,   # Programmatic_DMB_Metro_Warsaw (end 2099-12-31)
    3705905,   # Programmatic_TriPlay_Metro_Warszawa (end 2099-12-31)
    3705898,   # Programmatic_TriPlay_Metro_Warszawa (end 2099-12-31)
    3029333,   # Programmatic Roadside 12/12 (end 2078-12-31)
    3002440,   # Programmatic Reach Campaign 12/12 (end 2028-08-21)
}

# Rezerwacje calkowicie bez bookingu w Broadsign (brak proposal_id/contract_id/
# line_item_id JUZ W ZRODLE Control API, nie tylko w naszym fetchu -- potwierdzone
# 2026-07-22, patrz CLAUDE.md). Nie da sie ich dociagnac przez API, bo nie ma czego
# szukac. Recznie nadajemy syntetyczny campaign_id/line_item_id (pula 900000000+,
# poza zakresem prawdziwych ID Broadsign) zeby mialy resolvowalna nazwe w raportach
# zamiast pustego campaign_name. Wpiete w build_fact_play_logs.py (uzupelnia
# campaign_id/line_item_id po res_camp merge) + build_dim_campaign.py/
# build_dim_line_item.py (dokladaja syntetyczny wiersz z ta nazwa).
MANUAL_RESERVATION_OVERRIDES = {
    1125116202: {
        # TrainArrivingMessage -- czujka na stacji wykrywa nadjezdzajacy pociag i
        # przerywa standardowa petle komunikatem ostrzegawczym. Czas kontraktowo
        # nalezy do Metra Warszawskiego (nie do normalnego inventory reklamowego).
        "campaign_id": 900000001,
        "line_item_id": 900000001,
        "campaign_name": "Uwaga Pociąg",
        "line_item_name": "Uwaga Pociąg",
        "advertiser": "Metro Warszawskie",
        "client_name": "Metro Warszawskie",
    },
    # Pozostale 19 rezerwacji bez bookingu -- opisane w CLAUDE.md (2026-07-22),
    # wszystkie oznaczone recznie jako Autopromocja/Stroer (decyzja usera, nie
    # automatyczna regula -- tylko te konkretne reservation_id, nic wiecej).
    # Wspolny campaign_id (jedna "kampania" Autopromocja), unikalny line_item_id
    # per rezerwacja (zeby dim_line_item nie dostal duplikatu klucza).
    1230744166: {"campaign_id": 900000002, "line_item_id": 900000003, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1248739280: {"campaign_id": 900000002, "line_item_id": 900000004, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1248760574: {"campaign_id": 900000002, "line_item_id": 900000005, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1249965482: {"campaign_id": 900000002, "line_item_id": 900000006, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1249965517: {"campaign_id": 900000002, "line_item_id": 900000007, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1249965703: {"campaign_id": 900000002, "line_item_id": 900000008, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1320926616: {"campaign_id": 900000002, "line_item_id": 900000009, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1173115467: {"campaign_id": 900000002, "line_item_id": 900000010, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1173115610: {"campaign_id": 900000002, "line_item_id": 900000011, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1287527034: {"campaign_id": 900000002, "line_item_id": 900000012, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1422515259: {"campaign_id": 900000002, "line_item_id": 900000013, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1424533850: {"campaign_id": 900000002, "line_item_id": 900000014, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1444673694: {"campaign_id": 900000002, "line_item_id": 900000015, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1967182419: {"campaign_id": 900000002, "line_item_id": 900000016, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1967518300: {"campaign_id": 900000002, "line_item_id": 900000017, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1987096469: {"campaign_id": 900000002, "line_item_id": 900000018, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1989331182: {"campaign_id": 900000002, "line_item_id": 900000019, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1989334266: {"campaign_id": 900000002, "line_item_id": 900000020, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
    1991227199: {"campaign_id": 900000002, "line_item_id": 900000021, "campaign_name": "Autopromocja", "line_item_name": "Autopromocja", "advertiser": "Stroer", "client_name": "Stroer"},
}

# Rezerwacje serwisowe po reservation_id
SERWISOWY_RESERVATION_IDS = {
    1125116202,  # TrainArrivingMessage — systemowy komunikat
    1331907375,  # Promo Road mk2
    1243202200,  # Roadside_Promo
    1230744166,  # test_IT_synchro
}

# Line itemy serwisowe po line_item_id -- pozycje systemowe BEZ campaign_id
# (osierocone, nie ma ich w dim_campaign/dim_lineitem), wiec nie zlapie ich
# filtr po campaign_id. Lecą permanentnie na StroerTV i psuly realizacje.
# Flaga is_serwisowy=1 (dane zostaja, tylko oznaczone -- user filtruje w raporcie).
SERWISOWY_LINE_ITEM_IDS = {
    3270338,   # "TVP45s" -- 45s spot, permanentny (2025..2028), brak kampanii/statusu
    3355830,   # "Kino Letnie (15.V-31.VIII) STV" -- permanentny, brak kampanii/statusu
}

# Kampanie recznie wymuszone na is_serwisowy=2 (single-panel/testowe), mimo ze automatyczna
# regula get_single_panel_campaign_ids() ich nie lapie (np. brak play_logs w ogole -> 0
# unikalnych paneli, nie dokladnie 1, wiec regula "dokladnie 1 panel" nie zadziala).
FORCE_SINGLE_PANEL_CAMPAIGN_IDS = {
    2534697,   # OMD autopromocja -- 0 wierszy w play_logs, user chce is_serwisowy=2 (2026-07-14)
}

# Kampanie twardo wykluczone z dim_line_item / fact_campaign_budget (stare/nieaktualne dane,
# nie serwisowe -- to rozroznienie od SERWISOWY_CAMPAIGN_IDS: tamte zostaja w danych z flaga
# is_serwisowy=1, te znikaja calkowicie).
EXCLUDED_CAMPAIGN_IDS: set = {
    2472239,   # Sokoliki 30.V-05.VI -- stara kampania, perf_actual_repetitions absurdalnie
               # zawyzone (bez dopasowania do realnego play_log_player_id), 2026-07-13
}
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
