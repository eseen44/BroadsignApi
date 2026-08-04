# -*- coding: utf-8 -*-
"""Pomiar czasu i wynik KAZDEGO kroku pipeline'u -> jeden JSON na przebieg.

Do 2026-08-04 mierzylismy tylko czas per WARSTWA (trzy liczby: bronze, silver,
gold). `pipeline.log` mowil wiec "bronze zajal 3m21s", ale nie ktory z 22 krokow
bronze'u to zjadl. Ten modul dokłada pomiar per krok, zeby `Pipeline/graf.py
--status` mogl pokazac czasy, a nie tylko OK/FAIL.

Zalozenie projektowe: **kroki NIE zmieniaja swojej obslugi bledow.** Kazda
warstwa nadal sama łapie wyjatki i wpisuje `results[nazwa] = "OK" / "FAIL: ..."`.
Ten modul tylko mierzy czas (`mierz`) i na koniec zbiera stany (`dopisz_stany`).
Dzieki temu instrumentacja nie moze zmienic tego, CZY pipeline uznaje krok za
udany -- a to kod chodzacy codziennie z crona, wiec to wazniejsze niz elegancja.

Wszystkie trzy warstwy sa wolane w JEDNYM procesie (run_pipeline.py importuje
`run()` z kazdej), wiec jeden modulowy akumulator wystarczy -- nie ma potrzeby
scalac trzech plikow.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from time import perf_counter

ROOT = Path(__file__).resolve().parent.parent
PLIK = ROOT / "Data" / "_run_status.json"

CZASY: dict[str, float] = {}
STANY: dict[str, str] = {}
ZACZETE: str | None = None


@contextmanager
def mierz(nazwa: str):
    """Mierzy czas kroku. Wyjatkow NIE lapie -- to robi wolajacy.

    `finally` gwarantuje zapis czasu takze dla kroku, ktory padl: chcemy
    wiedziec, ze `play_logs_incremental` przewrocil sie po 4 minutach,
    a nie po sekundzie.
    """
    global ZACZETE
    if ZACZETE is None:
        ZACZETE = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = perf_counter()
    try:
        yield
    finally:
        CZASY[nazwa] = perf_counter() - t0


def dopisz_stany(results: dict[str, str], prefiks: str = "") -> None:
    """Zbiera `results` warstwy. `prefiks` dla Control API, gdzie klucze
    w `results` sa bez `ctrl_` (patrz fetch_control.fetch_all_control)."""
    for nazwa, stan in results.items():
        STANY[f"{prefiks}{nazwa}"] = stan


def zapisz(sciezka: Path | None = None) -> Path | None:
    """Jeden JSON na przebieg. Nie rzuca -- raportowanie nie ma prawa wywalic
    pipeline'u, ktory wlasnie policzyl dane."""
    try:
        p = sciezka or PLIK
        p.parent.mkdir(parents=True, exist_ok=True)
        kroki = [{"krok": k,
                  "stan": STANY.get(k, "?"),
                  "sekundy": round(CZASY.get(k, 0.0), 1)}
                 for k in CZASY]
        p.write_text(json.dumps(
            {"zaczete": ZACZETE or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             "kroki": kroki}, indent=2, ensure_ascii=False), encoding="utf-8")
        return p
    except Exception as e:                                  # noqa: BLE001
        print(f"  (status JSON nie zapisany: {type(e).__name__}: {e})")
        return None
