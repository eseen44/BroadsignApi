"""
Broadsign popstats client.
Zrodlo: https://popstats.broadsign.com/stroer_polska/
Format: pliki .txt lub .txt.gz (tab-separated), jeden dzien = jeden plik.
Dwa typy plikow:
  playlog-YYYY-MM-DD.txt[.gz]   — surowe logi emisji
  resources-YYYY-MM-DD.txt[.gz] — slownik ID->Nazwa dla wszystkich obiektow
"""
import io
import os
import gzip
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

URL_BASE = "https://popstats.broadsign.com/stroer_polska/"
USERNAME  = os.getenv("POPSTATS_USERNAME", "stroer_polska")
PASSWORD  = os.getenv("POPSTATS_PASSWORD")
if not PASSWORD:
    raise EnvironmentError("Brak POPSTATS_PASSWORD w .env")

# Mapowanie kolumn surowego pliku playlog .txt
RAW_COLS = {
    0:  "PlayerID",
    1:  "DateEndTime",
    2:  "Duration",
    3:  "AdCopyId",
    # 4: pomijamy (zawsze 2 — wewnetrzny kod Broadsign)
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


def _list_files(session: requests.Session, prefix: str) -> list[str]:
    """
    Zwraca posortowana liste plikow .txt o danym prefixie (playlog / resources).
    Ignoruje .txt.gz — serwer kompresuje starsze pliki, ale nasze dane
    przyrastaja codziennie przez pobranie biezacego .txt.
    Obsluga .gz jest zachowana w _fetch_text() jako safety net.
    """
    r = session.get(URL_BASE, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    files = []
    for a in soup.find_all("a"):
        href = a.get("href", "")
        low = href.lower()
        # tylko .txt, nie .txt.gz
        if prefix in low and low.endswith(".txt") and not low.endswith(".txt.gz"):
            files.append(href)
    return sorted(files)


def list_playlog_files(session: requests.Session) -> list[str]:
    """Zwraca posortowana liste biezacych plikow playlog (.txt)."""
    return _list_files(session, "playlog")


def list_resource_files(session: requests.Session) -> list[str]:
    """Zwraca posortowana liste biezacych plikow resources (.txt)."""
    return _list_files(session, "resources")


def _fetch_text(session: requests.Session, filename: str) -> str:
    """Pobiera zawartosc pliku — automatycznie dekompresuje .gz."""
    r = session.get(URL_BASE + filename, timeout=120)
    r.raise_for_status()
    if filename.lower().endswith(".gz"):
        return gzip.decompress(r.content).decode("utf-8")
    return r.text


def filename_to_date(filename: str) -> str:
    """
    'playlog-2026-05-26.txt'    -> '2026-05-26'
    'playlog-2026-05-22.txt.gz' -> '2026-05-22'
    """
    stem = filename
    for suffix in (".txt.gz", ".txt"):
        stem = stem.replace(suffix, "")
    stem = stem.replace("playlog-", "").replace("resources-", "")
    return stem


def fetch_and_parse(session: requests.Session, filename: str) -> "pd.DataFrame":
    """
    Pobiera jeden plik playlog (.txt lub .txt.gz) i zwraca zagregowany DataFrame.
    Grupowanie identyczne jak w oryginalnym ScriptLinux2.py + contract_id.
    """
    import pandas as pd

    text = _fetch_text(session, filename)

    df = pd.read_csv(
        io.StringIO(text),
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


def fetch_resources_latest(session: requests.Session) -> "pd.DataFrame":
    """
    Pobiera najnowszy plik resources i zwraca DataFrame z kolumnami:
      id   (str) — ID zasobu (display_unit, reservation, content, host, skin)
      name (str) — nazwa
      type (str) — typ: display_unit | reservation | content | host | skin

    Typy mapuja sie na kolumny play_logs:
      host         -> PlayerID        (ctrl_players.claimed_resource_id)
      display_unit -> DisplayUnitID   (ctrl_display_units.id)
      reservation  -> CampID          (ctrl_reservations.id)
      content      -> AdCopyId        (ctrl_content.id)
    """
    import pandas as pd

    files = list_resource_files(session)
    if not files:
        raise RuntimeError("Brak plikow resources na serwerze popstats")

    latest = files[-1]  # posortowane — ostatni = najnowszy
    print(f"  Pobieranie resources: {latest}")
    text = _fetch_text(session, latest)

    rows = []
    for line in text.splitlines():
        line = line.rstrip("\r\n")
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue  # niepelny wiersz — pomijamy
        rows.append(parts[:4])

    df = pd.DataFrame(rows, columns=["id", "name", "unused", "type"])
    df = df.drop(columns=["unused"])
    df["id"]   = df["id"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["type"] = df["type"].astype(str).str.strip()
    # Zostaw tylko znane typy
    known_types = {"display_unit", "reservation", "content", "host", "skin"}
    df = df[df["type"].isin(known_types)].copy()
    df["source_file"] = latest

    return df
