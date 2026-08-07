# -*- coding: utf-8 -*-
"""Wysylka raportu mailem przez relay SMTP.

STAN NA 2026-08-07: na VM NIE MA jeszcze relaya. Sprawdzone -- brak `msmtp`,
`sendmail`, `postfix`, brak `/etc/msmtprc`, a `smtp.stroeer.pl` sie nie
rozwiazuje. Host trzeba wziac od IT (to jedno pytanie przy okazji rozmowy
o koncie serwisowym do SQL Servera).

Do tego czasu droga jest taka: raport powstaje na VM, a wysylke robi Windows
przez COM Outlooka (`skills/broadsign-status-oficjalny/scripts/wyslij_mailem.ps1`).

Konfiguracja przez zmienne srodowiskowe -- ZERO poswiadczen w repo:
    SMTP_HOST   wymagane, np. relay.stroeer.pl
    SMTP_PORT   domyslnie 25
    SMTP_FROM   domyslnie raporty-danych@stroeer.pl
    SMTP_USER / SMTP_PASSWORD   opcjonalne; relaye wewnetrzne zwykle przyjmuja
                                poczte z zaufanej sieci BEZ uwierzytelnienia
"""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

from zrodla import Fakty


def tresc_html(f: Fakty) -> str:
    pozycje = []
    for p in f.potoki.values():
        if p.dostarczone:
            pozycje.append(f"<li><b>{p.nazwa}</b> — dostarczone "
                           f"{p.dostarczone:%H:%M}</li>")
        else:
            kiedy = (f", ostatnie dane {p.ostatnie_dobre:%d.%m %H:%M}"
                     if p.ostatnie_dobre else "")
            pozycje.append(f"<li><b>{p.nazwa}</b> — w trakcie aktualizacji{kiedy}</li>")

    if f.wszystko_ok:
        naglowek = "<p><b>Komplet danych dostarczony.</b></p>"
        stopka = ("<p>Raport w załączniku jest oczyszczony i nadaje się "
                  "do przesłania dalej.</p>")
    else:
        naglowek = "<p><b>Nie wszystkie dane dojechały.</b></p>"
        stopka = ("<p style='color:#8a5f10'><b>Raport w załączniku mówi prawdę o stanie, "
                  "ale nie nadaje się do przesłania dalej.</b></p>")

    l = f.liczby
    return (f"<div style=\"font-family:Segoe UI,sans-serif;font-size:14px;color:#171b1e\">"
            f"{naglowek}<ul>{''.join(pozycje)}</ul>"
            f"<p>{l.get('tabel_razem', 0)} tabel w hurtowni · "
            f"{l.get('wierszy_emisji', 0):,} wierszy emisji · "
            f"{l.get('organizacje', 0):,} organizacji</p>".replace(",", " ")
            + stopka +
            "<p style='color:#5a6469;font-size:12px'>Wiadomość wygenerowana automatycznie.</p>"
            "</div>")


def wyslij(do: str, temat: str, f: Fakty, zalaczniki: list[Path]) -> None:
    host = os.environ.get("SMTP_HOST")
    if not host:
        raise RuntimeError(
            "Brak SMTP_HOST. Na VM nie ma jeszcze relaya SMTP (stan 2026-08-07) — "
            "poproś IT o host przekaźnika. Do tego czasu wyślij raport z Windowsa: "
            "skills/broadsign-status-oficjalny/scripts/wyslij_mailem.ps1"
        )

    # Zalaczniki sprawdzamy PRZED polaczeniem -- lepiej wyjsc z bledem, niz
    # wyslac maila bez raportu. Cichy mail bez zalacznika wyglada jak sukces.
    for z in zalaczniki:
        if not z.exists() or z.stat().st_size < 10_000:
            raise RuntimeError(f"Zalacznik brakujacy albo podejrzanie maly: {z}")

    msg = EmailMessage()
    msg["From"] = os.environ.get("SMTP_FROM", "raporty-danych@stroeer.pl")
    msg["To"] = do
    msg["Subject"] = temat
    msg.set_content("Raport w załączniku (wersja HTML tej wiadomości zawiera podsumowanie).")
    msg.add_alternative(tresc_html(f), subtype="html")

    for z in zalaczniki:
        msg.add_attachment(z.read_bytes(), maintype="application", subtype="pdf",
                           filename=z.name)

    with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", "25")), timeout=60) as s:
        if (user := os.environ.get("SMTP_USER")) and (haslo := os.environ.get("SMTP_PASSWORD")):
            s.starttls()
            s.login(user, haslo)
        s.send_message(msg)
