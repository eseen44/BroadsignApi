# -*- coding: utf-8 -*-
"""Oficjalny raport dzienny: zbierz -> zloz -> PDF -> (opcjonalnie) wyslij.

    python3 Raport/raport.py                      # zloz HTML + PDF, nie wysylaj
    python3 Raport/raport.py --wyslij adres@...    # dodatkowo wyslij mailem

Uruchamiany na VM z crona po zakonczeniu obu pipeline'ow (emisyjny ~9:36,
sprzedazowy 7:30), wiec sensowna godzina to ~9:45.

## Zasada bezpieczenstwa

Raport jest OCZYSZCZONY (idzie poza zespol), ale nigdy nie klamie. Gdy
`Fakty.wszystko_ok` jest False:
  * raport i tak powstaje, z uczciwym stanem przy brakujacych obszarach,
  * ale `--wyslij` NIE wysyla go dalej -- wraca kod 2 i wolajacy (cron, czlowiek)
    decyduje sam.
Dzieki temu skrypt nie moze samodzielnie wypuscic raportu mowiacego, ze jest
dobrze, kiedy nie jest.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sklad import zloz                      # noqa: E402
from zrodla import zbierz_wszystko          # noqa: E402

WYJSCIE = Path(__file__).resolve().parent.parent / "Data" / "raport"

# Na VM jest `google-chrome` (sprawdzone 2026-08-07); na Windowsie Edge.
# Kolejnosc ma znaczenie -- pierwszy znaleziony wygrywa.
PRZEGLADARKI = [
    "google-chrome", "chromium", "chromium-browser",
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
]


def znajdz_przegladarke() -> str:
    for p in PRZEGLADARKI:
        if znaleziona := (shutil.which(p) or (p if Path(p).exists() else None)):
            return znaleziona
    raise RuntimeError(f"Nie znalazlem przegladarki do PDF. Szukalem: {PRZEGLADARKI}")


def do_pdf(html: Path, pdf: Path) -> None:
    """HTML -> PDF silnikiem przegladarki.

    `--no-pdf-header-footer` jest istotne: bez tego na kazdej stronie ladowalby
    `file:///...` i data systemowa. Sciezka lokalna na raporcie dla zarzadu
    wyglada jak wyciek z czyjegos pulpitu.
    """
    with tempfile.TemporaryDirectory() as profil:
        subprocess.run(
            [znajdz_przegladarke(), "--headless", "--disable-gpu", "--no-sandbox",
             "--no-pdf-header-footer", f"--user-data-dir={profil}",
             f"--print-to-pdf={pdf}", html.resolve().as_uri()],
            check=True, capture_output=True, timeout=180,
        )
    if not pdf.exists() or pdf.stat().st_size < 10_000:
        raise RuntimeError(f"PDF nie powstal albo jest podejrzanie maly: {pdf}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--wyslij", metavar="ADRES",
                    help="wyslij raport na ten adres (tylko gdy wszystko dojechalo)")
    ap.add_argument("--mimo-brakow", action="store_true",
                    help="wyslij TAKZE gdy czegos brakuje (raport bedzie mowil prawde, "
                         "ale nie nadaje sie do przekazania dalej)")
    args = ap.parse_args()

    fakty = zbierz_wszystko()
    teraz = datetime.now()

    print(f"=== Raport dzienny {teraz:%Y-%m-%d %H:%M} ===")
    for p in fakty.potoki.values():
        stan = p.dostarczone.strftime("%H:%M") if p.dostarczone else "BRAK"
        print(f"  {p.nazwa:<20} {stan:>8}  {p.uwaga}")
    print(f"  tabel: {fakty.liczby.get('tabel_razem')} "
          f"({', '.join(f'{k}={len(v)}' for k, v in fakty.tabele.items())})")
    for o in fakty.ostrzezenia:
        print(f"  OSTRZEZENIE: {o}")

    WYJSCIE.mkdir(parents=True, exist_ok=True)
    baza = f"Raport_dzienny_danych_{teraz:%Y-%m-%d_%H%M}"
    html, pdf = WYJSCIE / f"{baza}.html", WYJSCIE / f"{baza}.pdf"
    html.write_text(zloz(fakty, teraz), encoding="utf-8")
    do_pdf(html, pdf)
    print(f"  zapisano: {pdf} ({pdf.stat().st_size // 1024} KB)")

    if not args.wyslij:
        return 0
    if not fakty.wszystko_ok and not args.mimo_brakow:
        print("  NIE WYSYLAM: nie wszystko dojechalo. Raport jest na dysku i mowi prawde,\n"
              "               ale nie nadaje sie do przekazania dalej. --mimo-brakow wymusza.")
        return 2

    from wyslij import wyslij                                   # noqa: PLC0415
    temat = (f"Status danych — {teraz:%d.%m.%Y}" if fakty.wszystko_ok
             else f"[UWAGA] Status danych — {teraz:%d.%m.%Y}")
    wyslij(args.wyslij, temat, fakty, [pdf])
    print(f"  wyslano do {args.wyslij}: {temat}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
