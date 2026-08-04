# -*- coding: utf-8 -*-
"""Graf pipeline'u BroadsignApi w Mermaidzie -- struktura albo status przebiegu.

    "C:\\ProgramData\\Anaconda3\\python.exe" Pipeline/graf.py
    "C:\\ProgramData\\Anaconda3\\python.exe" Pipeline/graf.py --status pipeline.log

Odpowiednik `etl/run_all.py --graf` z repo SWAT Refactor, ale zrodla kroków sa
tu inne, bo pipeline jest inaczej zbudowany:

  silver, gold   maja DEKLARATYWNE listy `STEPS` -> importujemy je, wiec nie
                 moga sie rozjechac z rzeczywistoscia
  bronze         NIE ma jednej listy -- kroki sa rozsiane po ciele `main()`
                 w sekcjach (Direct API / Control API / play logi / resources /
                 MagicInfo / backup). Wyciagamy je wiec ze ZRODLA:
                   - `simple_steps` z `bronze/run_all.py` (regex)
                   - `ENDPOINTS` i `INCREMENTAL` z `bronze/fetch_control.py`
                     (import -- to zwykle stale)
                   - `NON_CRITICAL` z `bronze/run_all.py` (import)
                 plus kroki nazwane jawnie w kodzie (fill_rate, play_logs_*,
                 resources_latest, magicinfo_pop, bronze_backup_s3), ktore
                 trzymamy w `BRONZE_JAWNE` nizej.

SWIADOMIE nie przerabiamy bronze'a na deklaratywne STEPS: ten kod chodzi
codziennie z crona o 9:30 i przepisywanie jego przeplywu po to, zeby dalo sie
narysowac diagram, byloby zamiana realnego ryzyka na kosmetyke. Zamiast tego
`sprawdz_spojnosc()` porownuje wyliczona liste z etykietami faktycznie
wystepujacymi w zrodle i krzyczy, gdy sie rozejda.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

BRONZE_RUN_ALL = ROOT / "Pipeline" / "bronze" / "run_all.py"

# Kroki bronze'u nazwane w kodzie jednorazowo (nie w petli po liscie).
# (etykieta, opis)
BRONZE_JAWNE = [
    ("fill_rate", "fill rate per line item (screen_ids z bronze, nie 2. raz z API)"),
    ("ctrl_reservations_v22", "v22 po ID -- proposal_line_item_id, brak w incremental"),
    ("play_logs_historical", "import historyczny (jednorazowy, idempotentny)"),
    ("play_logs_incremental", "popstats -- rolling window ~2 mies."),
    ("resources_latest", "slownik nazw zasobow (ID -> nazwa)"),
    ("magicinfo_pop", "PoP z ekranow metro (TOTP, rolling ~31 dni)"),
    ("bronze_backup_s3", "kopia bronze na S3 (staging_bronze_backup)"),
]


def bronze_kroki() -> list[tuple[str, str, bool]]:
    """(etykieta, opis, krytyczny) w kolejnosci wykonania w `main()`."""
    from Pipeline.bronze.fetch_control import ENDPOINTS, INCREMENTAL
    from Pipeline.bronze.run_all import NON_CRITICAL

    src = BRONZE_RUN_ALL.read_text(encoding="utf-8")
    blok = re.search(r"simple_steps = \[(.*?)\]", src, re.S)
    proste = re.findall(r'\("([a-z_]+)",', blok.group(1)) if blok else []

    kroki: list[tuple[str, str, bool]] = []
    for k in proste:
        kroki.append((k, "Direct API (overwrite)", k not in NON_CRITICAL))
    kroki.append(("fill_rate", dict(BRONZE_JAWNE)["fill_rate"], True))
    for tabela in ENDPOINTS:
        tryb = "incremental (kursor)" if tabela in INCREMENTAL else "overwrite"
        kroki.append((f"ctrl_{tabela}", f"Control API -- {tryb}", True))
    for etykieta, opis in BRONZE_JAWNE:
        if etykieta == "fill_rate":
            continue
        kroki.append((etykieta, opis, etykieta not in NON_CRITICAL))
    return kroki


def silver_kroki() -> list[tuple[str, str, bool]]:
    from Pipeline.silver.run_all import STEPS
    return [(n, opis, True) for n, _, opis in STEPS]


def gold_kroki() -> list[tuple[str, str, bool]]:
    from Pipeline.gold.run_all import STEPS
    return [(n, "", True) for n, _ in STEPS]


def sprawdz_spojnosc() -> list[str]:
    """Czy lista krokow bronze'u zgadza sie ze zrodlem.

    Lapie sytuacje, gdy ktos dopisal krok w `main()` i nie ruszyl
    `BRONZE_JAWNE` -- graf pokazalby wtedy niepelny pipeline.

    Etykiety trafiaja do `results` na DWA sposoby, wiec porownujemy z suma obu:
      - literalem: `results["fill_rate"] = ...`
      - przez zmienna w petli: `results[label] = ...` (kroki z `simple_steps`)
    Pomijamy `ctrl_*` pochodzace z `ENDPOINTS` -- te ida f-stringiem
    (`results[f"ctrl_{name}"]`), wiec w zrodle nie ma ich jako literalow.
    `ctrl_reservations_v22` JEST literalem i celowo NIE jest pomijany
    (pierwsza wersja tej kontroli wykluczala go razem z reszta `ctrl_*`
    i przez to dawala falszywy alarm).
    """
    from Pipeline.bronze.fetch_control import ENDPOINTS

    src = BRONZE_RUN_ALL.read_text(encoding="utf-8")
    literaly = set(re.findall(r'results\["([a-z_0-9]+)"\]', src))
    blok = re.search(r"simple_steps = \[(.*?)\]", src, re.S)
    z_petli = set(re.findall(r'\("([a-z_]+)",', blok.group(1))) if blok else set()
    w_zrodle = literaly | z_petli

    z_endpoints = {f"ctrl_{t}" for t in ENDPOINTS}
    wyliczone = {k for k, _, _ in bronze_kroki()} - z_endpoints

    problemy = []
    if brakujace := sorted(w_zrodle - wyliczone):
        problemy.append(f"w zrodle bronze/run_all.py sa kroki, ktorych graf NIE zna: {brakujace}")
    if nadmiarowe := sorted(wyliczone - w_zrodle):
        problemy.append(f"graf zna kroki nieobecne w zrodle: {nadmiarowe}")
    return problemy


def status_z_logu(sciezka: Path) -> dict[str, str]:
    """Wynik ostatniego przebiegu z `pipeline.log`.

    Log ma format `  OK   <krok>` / `  FAIL  <krok>` / `  POMINIETE <krok>`,
    a kazdy przebieg zaczyna sie `==== <data> START ====`. Bierzemy TYLKO
    ostatni blok -- inaczej stary sukces przyslonilby dzisiejsza awarie.
    """
    tekst = sciezka.read_text(encoding="utf-8", errors="ignore")
    starty = [m.start() for m in re.finditer(r"==== .* START ====", tekst)]
    if starty:
        tekst = tekst[starty[-1]:]
    stan: dict[str, str] = {}
    for m in re.finditer(r"^\s+(OK|FAIL[^\s]*|POMINIETE)\s+([a-z_0-9]+)", tekst, re.M):
        surowy, krok = m.group(1), m.group(2)
        stan[krok] = "OK" if surowy == "OK" else ("POMINIETE" if surowy == "POMINIETE" else "FAIL")
    return stan


WARSTWY = [
    ("BRONZE", "fetch z API -> parquet", bronze_kroki),
    ("SILVER", "join + wzbogacenie", silver_kroki),
    ("GOLD", "star schema dla PBI", gold_kroki),
]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--status", metavar="PIPELINE_LOG", default=None,
                    help="pokoloruj wynikiem z pipeline.log (sciezka do logu)")
    args = ap.parse_args()

    problemy = sprawdz_spojnosc()
    for p in problemy:
        print(f"%% UWAGA: {p}", file=sys.stderr)

    stan: dict[str, str] = {}
    if args.status:
        sciezka = Path(args.status)
        if not sciezka.exists():
            print(f"BLAD: nie ma pliku {sciezka}", file=sys.stderr)
            return 1
        stan = status_z_logu(sciezka)
        if not stan:
            print("BLAD: w logu nie znalazlem zadnych krokow "
                  "(spodziewam sie linii typu '  OK   dim_date')", file=sys.stderr)
            return 1

    print("```mermaid")
    print("flowchart TD")
    if stan:
        # Kolor tekstu JAWNIE -- bez tego Mermaid dobiera go wg motywu i na
        # kolorowym tle potrafi wyjsc tekst w kolorze wypelnienia.
        print("  classDef ok fill:#dff0e5,stroke:#1f7a4d,color:#10321f;")
        print("  classDef fail fill:#f7dcdc,stroke:#a52b2b,color:#3d1010,stroke-width:2px;")
        print("  classDef skip fill:#eeeeee,stroke:#a8a8a8,color:#5c5c5c,stroke-dasharray:4 3;")
        print("  classDef nieznany fill:#f4f6f8,stroke:#b9c2cc,color:#5c6672;")

    wszystkie: list[tuple[str, str]] = []          # (id_wezla, krok)
    for kod, (nazwa, podpis, fn) in enumerate(WARSTWY):
        kroki = fn()
        print(f'  subgraph W{kod}["{nazwa} — {podpis} ({len(kroki)} kroków)"]')
        for i, (krok, _opis, kryt) in enumerate(kroki):
            wid = f"w{kod}_{i}"
            wszystkie.append((wid, krok))
            otw, zam = ("[", "]") if kryt else ("(", ")")
            gwiazdka = "" if kryt else " *"
            print(f'    {wid}{otw}"{krok}{gwiazdka}"{zam}')
        print("  end")

    print('  subgraph SYNC["SYNC — dokad ida wyniki"]')
    print('    onedrive["OneDrive / UbuntuSynch<br/>-> SharePoint -> Power BI"]')
    print('    athena["S3 / Athena<br/>broadsign/gold"]')
    print("  end")

    # Krawedzie MIEDZY warstwami (bronze -> silver -> gold -> sync).
    # Wewnatrz warstwy kroki sa niezalezne -- kazdy pobiera/buduje wlasna tabele.
    print("  W0 --> W1")
    print("  W1 --> W2")
    print("  W2 --> onedrive")
    print("  W2 --> athena")

    if stan:
        for klasa in ("ok", "fail", "skip", "nieznany"):
            czlonkowie = [wid for wid, krok in wszystkie
                          if {"OK": "ok", "FAIL": "fail", "POMINIETE": "skip"}
                          .get(stan.get(krok), "nieznany") == klasa]
            if czlonkowie:
                print(f'  class {",".join(czlonkowie)} {klasa}')
    print("```")

    if stan:
        braki = [k for _, k in wszystkie if k not in stan]
        if braki:
            print(f"%% krokow bez wpisu w logu: {len(braki)} -> {braki}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
