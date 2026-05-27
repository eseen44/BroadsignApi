"""
Broadsign popstats client.
Źródło: https://popstats.broadsign.com/stroer_polska/
Format: pliki .txt (tab-separated), jeden dzień = jeden plik.
Dane: surowe logi emisji (każde odtworzenie = jeden wiersz).
"""
import io
import requests
from bs4 import BeautifulSoup
from datetime import date

URL_BASE = "https://popstats.broadsign.com/stroer_polska/"
USERNAME  = "stroer_polska"
PASSWORD  = "***REMOVED***"

# Mapowanie kolumn surowego pliku .txt
RAW_COLS = {
    0:  "PlayerID",
    1:  "DateEndTime",
    2:  "Duration",
    3:  "AdCopyId",
    # 4: pomijamy (zawsze 2 — wewnętrzny kod Broadsign)
    5:  "CampID",
    6:  "FrameID",
    7:  "DisplayUnitID",
    8:  "Impresje",
    # 9-11: puste / techniczne
    12: "_json_meta",   # JSON z contract_id
}


def get_session() -> requests.Session:
    session = requests.Session()
    session.auth = (USERNAME, PASSWORD)
    return session


def list_playlog_files(session: requests.Session) -> list[str]:
    """Zwraca posortowaną listę dostępnych plików playlog (np. ['playlog-2026-05-26.txt'])."""
    r = session.get(URL_BASE, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    files = [
        a.get("href") for a in soup.find_all("a")
        if a.get("href", "").lower().endswith(".txt")
        and "playlog" in a.get("href", "").lower()
    ]
    return sorted(files)


def filename_to_date(filename: str) -> str:
    """'playlog-2026-05-26.txt' → '2026-05-26'"""
    stem = filename.replace("playlog-", "").replace(".txt", "")
    return stem


def fetch_and_parse(session: requests.Session, filename: str) -> "pd.DataFrame":
    """
    Pobiera jeden plik playlog i zwraca zagregowany DataFrame
    (grupowanie identyczne jak w oryginalnym Script.py, + contract_id).
    """
    import pandas as pd

    r = session.get(URL_BASE + filename, timeout=120)
    r.raise_for_status()

    df = pd.read_csv(
        io.StringIO(r.text),
        sep="\t",
        header=None,
        names=range(13),
        on_bad_lines="skip",
        low_memory=False,
    )

    # Wyciągnij contract_id z JSON (kolumna 12)
    df["contract_id"] = df[12].astype(str).str.extract(r'"contract_id"\s*:\s*"([^"]+)"')

    # Rzutowanie typów
    for col in [0, 2, 3, 5, 6, 7, 8]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df[1] = pd.to_datetime(df[1], errors="coerce")

    # Rename
    df = df.rename(columns={
        0: "PlayerID",
        1: "DateEndTime",
        2: "Duration",
        3: "AdCopyId",
        5: "CampID",
        6: "FrameID",
        7: "DisplayUnitID",
        8: "Impresje",
    })

    df["DateEnd"]   = df["DateEndTime"].dt.date.astype(str)
    df["timeslot"]  = df["DateEndTime"].dt.hour
    df["emisje"]    = 1

    # Grupowanie (identyczne jak Script.py, + contract_id)
    agg = df.groupby(
        ["PlayerID", "DateEnd", "timeslot", "AdCopyId", "CampID",
         "FrameID", "DisplayUnitID", "contract_id"],
        as_index=False,
        dropna=False,
    ).agg(
        Duration=("Duration", "sum"),
        Impresje=("Impresje", "sum"),
        emisje=("emisje", "sum"),
    )

    agg["timeslot_label"] = agg["timeslot"].apply(
        lambda h: f"{int(h):02d}:00-{(int(h)+1)%24:02d}:00" if pd.notnull(h) else None
    )

    return agg
