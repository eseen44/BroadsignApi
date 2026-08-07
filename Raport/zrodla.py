# -*- coding: utf-8 -*-
"""Zbieranie faktow do oficjalnego raportu dziennego.

Wszystkie cztery zrodla sa osiagalne Z VM-KI (zweryfikowane 2026-08-07), wiec
raport da sie zlozyc bez udzialu laptopa:

  BroadsignApi   /dane/BroadsignApi/pipeline.log            lokalnie
  SWAT           s3://<bucket>/swat/_run_status/_run_status.json
                 -- SWAT liczy sie dzis na Windowsie, ale jego `run_all.py`
                    wypycha status na S3 WLASNIE po to, zeby dalo sie go
                    odczytac z innej maszyny niz liczaca
  Organizacje    /dane/Pipedrive/Data/orgs_export.log       lokalnie
  Wykaz tabel    listing S3 po trzech prefiksach            (patrz PREFIKSY)

Ten modul TYLKO czyta i parsuje. Sklad HTML jest w `sklad.py`, zeby dalo sie
testowac jedno bez drugiego (parsowanie na przykladowym logu, sklad na
sztucznym slowniku).
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

BUCKET = os.environ.get("S3_DATA_BUCKET", "stroeer-samm-data-warehouse-non-samm-prod")

LOG_BROADSIGN = Path("/dane/BroadsignApi/pipeline.log")
LOG_ORGS = Path("/dane/Pipedrive/Data/orgs_export.log")
PARQUET_ORGS = Path("/dane/Pipedrive/Data/organizations.parquet")

KLUCZ_STATUS_SWAT = "swat/_run_status/_run_status.json"

# Trzy prefiksy = trzy warstwy. Rozdzielone, bo kazda idzie do innej sekcji
# raportu, a `swat/gold/` jest ZAGNIEZDZONY w `swat/` -- naiwny jeden listing
# ze wzorcem `swat/([^/]+)/data/` gubi cala warstwe gold (18 tabel).
PREFIKSY = {
    "emisyjne": "broadsign/gold/",
    "swat_robocza": "swat/",
    "swat_model": "swat/gold/",
}


@dataclass
class Potok:
    """Wynik jednego potoku. `dostarczone` = None znaczy: dzis nie dojechalo."""
    nazwa: str
    dostarczone: datetime | None = None
    krokow_ok: int = 0
    krokow_razem: int = 0
    ostatnie_dobre: datetime | None = None
    uwaga: str = ""

    @property
    def ok(self) -> bool:
        return self.dostarczone is not None and self.krokow_ok == self.krokow_razem


@dataclass
class Fakty:
    potoki: dict[str, Potok] = field(default_factory=dict)
    liczby: dict[str, int] = field(default_factory=dict)
    tabele: dict[str, list[str]] = field(default_factory=dict)
    ostrzezenia: list[str] = field(default_factory=list)

    @property
    def wszystko_ok(self) -> bool:
        return bool(self.potoki) and all(p.ok for p in self.potoki.values())


# ---------------------------------------------------------------------------
# 1. BroadsignApi -- z wlasnego logu
# ---------------------------------------------------------------------------

_RE_DONE = re.compile(r"==== (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) DONE ====")
_RE_START = re.compile(r"==== (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) START ====")
_RE_UPSERT = re.compile(r"\[upsert_date\] \d{4}-\d{2}-\d{2}: ([\d ]+) wierszy -> lacznie ([\d ]+) w play_logs")
_RE_KAMPANIE = re.compile(r"^\s*Kampanie: (\d+)\s*$", re.M)


def zbierz_broadsign(log: Path = LOG_BROADSIGN) -> tuple[Potok, dict[str, int]]:
    p = Potok("Dane emisyjne")
    liczby: dict[str, int] = {}
    if not log.exists():
        p.uwaga = "brak logu przebiegu"
        return p, liczby

    tekst = log.read_text(encoding="utf-8", errors="replace")

    # Bierzemy OSTATNI przebieg, nie caly plik: log jest dopisywany od miesiecy,
    # wiec wzorce spoza dzisiejszej sekcji dalyby wczorajsze liczby.
    starty = list(_RE_START.finditer(tekst))
    if not starty:
        p.uwaga = "log bez znacznika START"
        return p, liczby
    ostatni = tekst[starty[-1].start():]

    done = _RE_DONE.search(ostatni)
    if done:
        p.dostarczone = datetime.strptime(done.group(1), "%Y-%m-%d %H:%M:%S")
    else:
        # Sekcja bez DONE = przebieg przerwany albo trwajacy. Ostatni DONE
        # w CALYM pliku mowi, kiedy dane byly ostatnio dobre.
        wszystkie = _RE_DONE.findall(tekst)
        if wszystkie:
            p.ostatnie_dobre = datetime.strptime(wszystkie[-1], "%Y-%m-%d %H:%M:%S")
        p.uwaga = "przebieg nie zakonczyl sie znacznikiem DONE"

    # UWAGA: to NIE jest liczba krokow pipeline'u (tych jest 38). Log wypisuje
    # `OK` takze przy kazdej wyslanej tabeli i przy synchronizacjach, wiec
    # wychodzi ~68. Traktujemy to jako SYGNAL ZDROWIA (`ok` = zero FAIL-i),
    # nie jako licznik do pokazania -- raport oficjalny i tak go nie wyswietla.
    p.krokow_ok = len(re.findall(r"^\s*OK\s+\S", ostatni, re.M))
    p.krokow_razem = p.krokow_ok + len(re.findall(r"^\s*FAIL\s+\S", ostatni, re.M))

    if m := _RE_UPSERT.search(ostatni):
        liczby["przyrost_emisji"] = int(m.group(1).replace(" ", ""))
        liczby["wierszy_emisji"] = int(m.group(2).replace(" ", ""))
    if m := _RE_KAMPANIE.search(ostatni):
        liczby["kampanie"] = int(m.group(1))
    return p, liczby


# ---------------------------------------------------------------------------
# 2. SWAT -- status z S3 (dziala niezaleznie od tego, gdzie SWAT sie liczy)
# ---------------------------------------------------------------------------

def zbierz_swat(bucket: str = BUCKET) -> Potok:
    import boto3
    p = Potok("Dane sprzedażowe")
    try:
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=KLUCZ_STATUS_SWAT)
        d = json.loads(obj["Body"].read())
    except Exception as e:                        # noqa: BLE001 -- raport ma powstac mimo wszystko
        p.uwaga = f"nie udalo sie odczytac statusu: {type(e).__name__}"
        return p

    kroki = d.get("kroki", [])
    p.krokow_razem = len(kroki)
    p.krokow_ok = sum(1 for k in kroki if k.get("stan") == "OK")
    start = datetime.strptime(d["zaczete"], "%Y-%m-%d %H:%M:%S")
    koniec = start + timedelta(seconds=sum(k.get("sekundy", 0) for k in kroki))

    if p.krokow_ok == p.krokow_razem:
        p.dostarczone = koniec
    else:
        p.ostatnie_dobre = start
        zle = [k["krok"] for k in kroki if k.get("stan", "").startswith("FAIL")]
        p.uwaga = f"przebieg przerwany na kroku {zle[0]}" if zle else "przebieg niekompletny"
    return p


# ---------------------------------------------------------------------------
# 3. Organizacje -- z logu eksportu
# ---------------------------------------------------------------------------

_RE_ORG = re.compile(r"Pobrano (\d+) organizacji")
_RE_AKT = re.compile(r"Aktywności: (\d+) wykonanych")


def zbierz_organizacje(log: Path = LOG_ORGS,
                       parquet: Path = PARQUET_ORGS) -> tuple[Potok, dict[str, int]]:
    p = Potok("Baza klientów")
    liczby: dict[str, int] = {}
    if not log.exists():
        p.uwaga = "brak logu eksportu"
        return p, liczby

    tekst = log.read_text(encoding="utf-8", errors="replace")
    # Log jest nadpisywany co przebieg, ale bierzemy ostatnie trafienie na
    # wypadek, gdyby kiedys zaczal byc dopisywany.
    if m := list(_RE_ORG.finditer(tekst)):
        liczby["organizacje"] = int(m[-1].group(1))
    if m := list(_RE_AKT.finditer(tekst)):
        liczby["aktywnosci"] = int(m[-1].group(1))

    # Godzine dostawy bierzemy z PLIKU, nie z logu: log konczy sie linia
    # o pominietym uploadzie na SharePoint (to normalne, patrz README skilla),
    # a o dostarczeniu swiadczy swiezy parquet.
    if parquet.exists():
        p.dostarczone = datetime.fromtimestamp(parquet.stat().st_mtime)
        p.krokow_ok = p.krokow_razem = 1
    else:
        p.uwaga = "brak pliku organizations.parquet"
    return p, liczby


# ---------------------------------------------------------------------------
# 4. Wykaz tabel -- listing S3
# ---------------------------------------------------------------------------

# Techniczne -> biznesowe. Klucz to nazwa katalogu na S3 (dla warstwy gold SWAT
# BEZ prefiksu `swat_gold_`, zdejmowanego nizej).
NAZWY = {
    # broadsign/gold
    "fact_play_logs": "Emisje",
    "fact_campaign_budget": "Budżet i realizacja kampanii",
    "fact_health": "Kondycja kampanii",
    "dim_campaign": "Kampanie",
    "dim_line_item": "Pozycje kampanii",
    "dim_player": "Ekrany i odtwarzacze",
    "dim_content": "Treści reklamowe",
    "dim_date": "Kalendarz",
    "dim_campaign_period": "Okresy kampanii",
    # swat/ (warstwa robocza)
    "sales": "Sprzedaż",
    "rezerwacje": "Rezerwacje powierzchni",
    "kantar": "Wydatki Kantara",
    "kantar_medium_agg": "Wydatki wg mediów",
    "kantar_ams_others": "Wydatki pozostałe",
    "campaign_adjustment": "Uzgodnienia kontraktów",
    "budget_format_blok": "Budżet wg formatu i bloku",
    "budget_share_fbk": "Udziały budżetowe",
    "budget_per_panel_type": "Budżet wg typu panelu",
    "budget_per_panel": "Budżet wg panelu",
    "tabela_wynikowa": "Tabela wynikowa",
    "nip_reconciliation": "Uzgodnienia NIP",
    "dim_panels": "Panele",
    "dim_format": "Formaty",
    "ims_total": "Powierzchnie IMS",
    "lista_pakietowa_2025": "Lista pakietowa 2025",
    "lista_pakietowa_2026": "Lista pakietowa 2026",
    "pakiety_ooh": "Pakiety OOH",
    "pakiety_wiaty": "Pakiety wiaty",
    "pakiety_metro": "Pakiety metro",
    "dim_campaign_swat": "Kampanie",
    "dim_client": "Klienci",
    "dim_account": "Opiekunowie",
    "dim_category": "Kategorie",
    "dim_material": "Materiały",
    "dim_material_group": "Grupy materiałów",
    "dim_planner": "Planiści",
    # swat/gold (model analityczny)
    "fact_sales": "Sprzedaż",
    "fact_budget": "Budżety",
    "fact_rezerwacje": "Rezerwacje",
    "fact_kantar": "Wydatki rynkowe",
    "dim_agencja": "Agencje",
    "dim_blok": "Bloki kalendarzowe",
    "dim_panel": "Panele",
    "dim_panel_wersja": "Wersje paneli",
    "dim_kantar_medium": "Media (Kantar)",
    "dim_kantar_produkt": "Produkty (Kantar)",
    "dim_kantar_kreacja": "Kreacje (Kantar)",
}


def zbierz_tabele(bucket: str = BUCKET) -> tuple[dict[str, list[str]], list[str]]:
    """Zwraca {warstwa: [nazwy biznesowe]} oraz liste ostrzezen.

    Nazwa nieznana NIE trafia do raportu pod postacia techniczna -- zamiast tego
    laduje w ostrzezeniach. Pokazanie biznesowi `swat_gold_dim_costam` jest
    gorsze niz pominiecie jednej pozycji, a ostrzezenie wymusza dopisanie
    jej do slownika NAZWY.
    """
    import boto3
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    ostrzezenia: list[str] = []
    wynik: dict[str, list[str]] = {}

    surowe: dict[str, set[str]] = {k: set() for k in PREFIKSY}
    for warstwa, prefiks in PREFIKSY.items():
        wzor = re.compile(re.escape(prefiks) + r"([^/]+)/data/")
        for strona in paginator.paginate(Bucket=bucket, Prefix=prefiks):
            for it in strona.get("Contents", []):
                if m := wzor.match(it["Key"]):
                    surowe[warstwa].add(m.group(1))

    # `swat/` zlapal takze katalogi warstwy gold i `_run_status` -- odejmujemy,
    # inaczej te same tabele policzylyby sie dwa razy.
    surowe["swat_robocza"] -= {"gold", "_run_status"}

    for warstwa, nazwy in surowe.items():
        biznesowe = []
        for n in sorted(nazwy):
            klucz = n[len("swat_gold_"):] if n.startswith("swat_gold_") else n
            if czytelna := NAZWY.get(klucz):
                biznesowe.append(czytelna)
            else:
                ostrzezenia.append(f"tabela bez nazwy biznesowej: {warstwa}/{n}")
        wynik[warstwa] = biznesowe
    return wynik, ostrzezenia


# ---------------------------------------------------------------------------

def zbierz_wszystko() -> Fakty:
    f = Fakty()
    bs, l1 = zbierz_broadsign()
    org, l2 = zbierz_organizacje()
    swat = zbierz_swat()
    # Kolejnosc jak w raporcie: emisyjne, baza klientow, sprzedazowe.
    f.potoki = {"emisyjne": bs, "klienci": org, "sprzedazowe": swat}
    f.liczby = {**l1, **l2}

    f.tabele, f.ostrzezenia = zbierz_tabele()
    f.liczby["tabel_razem"] = sum(len(v) for v in f.tabele.values())
    for p in f.potoki.values():
        if p.uwaga:
            f.ostrzezenia.append(f"{p.nazwa}: {p.uwaga}")
    return f


if __name__ == "__main__":
    fakty = zbierz_wszystko()
    print(f"wszystko OK: {fakty.wszystko_ok}")
    for k, p in fakty.potoki.items():
        print(f"  {p.nazwa:<20} dostarczone={p.dostarczone} "
              f"{p.krokow_ok}/{p.krokow_razem} {p.uwaga}")
    print("  liczby:", fakty.liczby)
    print("  tabele:", {k: len(v) for k, v in fakty.tabele.items()})
    for o in fakty.ostrzezenia:
        print("  OSTRZEZENIE:", o)
