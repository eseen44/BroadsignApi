# -*- coding: utf-8 -*-
"""Sklad oficjalnego raportu dziennego z faktow zebranych przez `zrodla.py`.

Ten modul NIE czyta zadnych logow ani S3 -- dostaje gotowy obiekt `Fakty`.
Dzieki temu da sie go testowac na sztucznym slowniku, takze dla dnia, w ktorym
cos nie dojechalo (a takich dni nie da sie zamowic na zadanie).

## Uczciwosc raportu

Raport idzie poza zespol, wiec jest OCZYSZCZONY -- bez nazw technicznych, bez
awarii, bez otwartych spraw. Ale oczyszczony to nie to samo co nieprawdziwy:

  * gdy wszystko dojechalo  -> naglowek "Komplet danych dostarczony na czas"
  * gdy czegos brakuje      -> ten obszar dostaje uczciwy stan "W trakcie
                               aktualizacji" z data ostatnich dobrych danych,
                               a `Fakty.wszystko_ok` jest False, na co wolajacy
                               reaguje NIEWYSYLANIEM raportu dalej

Nigdy nie wpisujemy zdan typu "zero incydentow" czy "100% dostepnosci" -- to
twierdzenia o czyms, czego swiadomie nie pokazujemy, czyli klamstwo, a nie
pominiecie.
"""
from __future__ import annotations

from datetime import datetime

from zrodla import Fakty, Potok

OK_TLO, OK_LINIA, OK_TEKST = "#dff0e5", "#1f7a4d", "#10321f"
SRC_TLO, SRC_LINIA, SRC_TEKST = "#eaeef1", "#93a1ac", "#333c44"
STRZALKA = "#7a8792"
SANS = "ui-sans-serif, Segoe UI, sans-serif"

MIESIACE = ["stycznia", "lutego", "marca", "kwietnia", "maja", "czerwca",
            "lipca", "sierpnia", "września", "października", "listopada", "grudnia"]


def po_polsku(d: datetime) -> str:
    return f"{d.day} {MIESIACE[d.month - 1]} {d.year}"


def sep(n: int) -> str:
    """Separator tysiecy waska spacja nierozdzielajaca -- liczba nigdy sie nie zlamie."""
    return f"{n:,}".replace(",", " ")


# --------------------------------------------------------------------------
# SVG: zrodla -> lancuch etapow -> odbiorcy
# --------------------------------------------------------------------------

def _txt(x, y, s, rozmiar=11, waga=500, kolor=SRC_TEKST):
    return (f'<text x="{x:.0f}" y="{y:.0f}" text-anchor="middle" font-size="{rozmiar}" '
            f'font-weight="{waga}" fill="{kolor}" font-family="{SANS}">{s}</text>')


def _box(x, y, w, h, linie, tlo, linia, kolor, waga=1.0):
    s = [f'<rect x="{x:.0f}" y="{y:.0f}" width="{w}" height="{h}" rx="3" fill="{tlo}" '
         f'stroke="{linia}" stroke-width="{waga}"/>']
    n = len(linie)
    for i, (tekst, rozm, gr) in enumerate(linie):
        s.append(_txt(x + w / 2, y + h / 2 + (i - (n - 1) / 2) * 15 + 4, tekst, rozm, gr, kolor))
    return "".join(s)


def svg_potok(zrodla, etapy, wyniki, aria, ident):
    W, x_src, w_src = 1046, 8, 222
    w_et, gap_et, x_et0 = 152, 26, 268
    x_out, w_out = 812, 226
    h_src, gap_src, h_et = 46, 10, 64

    wys_src = len(zrodla) * (h_src + gap_src) - gap_src
    wys_out = len(wyniki) * (h_src + gap_src) - gap_src
    H = max(wys_src, wys_out, h_et) + 28
    sr = H / 2
    grot = f"g{ident}"

    s = [f'<svg viewBox="0 0 {W} {H:.0f}" width="100%" xmlns="http://www.w3.org/2000/svg" '
         f'role="img" aria-label="{aria}">',
         f'<defs><marker id="{grot}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
         f'markerHeight="7" orient="auto-start-reverse">'
         f'<path d="M0,1 L9,5 L0,9 z" fill="{STRZALKA}"/></marker></defs>']

    x_et = [x_et0 + i * (w_et + gap_et) for i in range(len(etapy))]
    y0, y1 = sr - wys_src / 2, sr - wys_out / 2

    for i in range(len(zrodla)):
        cy = y0 + i * (h_src + gap_src) + h_src / 2
        s.append(f'<path d="M {x_src + w_src} {cy:.0f} C {x_src + w_src + 26} {cy:.0f}, '
                 f'{x_et[0] - 26} {sr:.0f}, {x_et[0] - 4} {sr:.0f}" fill="none" '
                 f'stroke="{STRZALKA}" stroke-width="1.3" marker-end="url(#{grot})"/>')
    for i in range(len(etapy) - 1):
        s.append(f'<line x1="{x_et[i] + w_et}" y1="{sr:.0f}" x2="{x_et[i+1] - 4}" y2="{sr:.0f}" '
                 f'stroke="{STRZALKA}" stroke-width="1.3" marker-end="url(#{grot})"/>')
    for i in range(len(wyniki)):
        cy = y1 + i * (h_src + gap_src) + h_src / 2
        s.append(f'<path d="M {x_et[-1] + w_et} {sr:.0f} C {x_et[-1] + w_et + 26} {sr:.0f}, '
                 f'{x_out - 26} {cy:.0f}, {x_out - 4} {cy:.0f}" fill="none" '
                 f'stroke="{STRZALKA}" stroke-width="1.3" marker-end="url(#{grot})"/>')

    for i, z in enumerate(zrodla):
        s.append(_box(x_src, y0 + i * (h_src + gap_src), w_src, h_src,
                      [(z, 11, 500)], SRC_TLO, SRC_LINIA, SRC_TEKST))
    for i, (nazwa, pod) in enumerate(etapy):
        s.append(_box(x_et[i], sr - h_et / 2, w_et, h_et,
                      [(nazwa, 12, 700), (pod, 10, 400)], OK_TLO, OK_LINIA, OK_TEKST, 1.2))
    for i, w in enumerate(wyniki):
        s.append(_box(x_out, y1 + i * (h_src + gap_src), w_out, h_src,
                      [(w, 11, 600)], OK_TLO, OK_LINIA, OK_TEKST))
    s.append("</svg>")
    return "".join(s)


# --------------------------------------------------------------------------

def kafel(p: Potok) -> str:
    if p.dostarczone:
        stan = "Dostarczona" if p.nazwa.endswith("klientów") else "Dostarczone"
        kiedy = p.dostarczone.strftime("%d.%m.%Y, godz. %H:%M")
        klasa = ""
    else:
        stan = "W trakcie aktualizacji"
        kiedy = (f"ostatnie dane: {p.ostatnie_dobre.strftime('%d.%m.%Y, godz. %H:%M')}"
                 if p.ostatnie_dobre else "brak danych o ostatnim odświeżeniu")
        klasa = " czeka"
    return (f'<div class="verdict{klasa}"><span class="name">{p.nazwa}</span>'
            f'<span class="score">{stan}</span><span class="when">{kiedy}</span></div>')


def wiersz_dostepnosci(obszar, zakres, p: Potok, gdzie) -> str:
    if p.dostarczone:
        akt, pigulka, klasa = p.dostarczone.strftime("%d.%m.%Y"), "Aktualne", "ok"
    else:
        akt = p.ostatnie_dobre.strftime("%d.%m.%Y") if p.ostatnie_dobre else "—"
        pigulka, klasa = "W aktualizacji", "czeka"
    return (f'<tr><td>{obszar}</td><td>{zakres}</td><td class="when">{akt}</td>'
            f'<td>{gdzie}</td><td><span class="pill {klasa}">{pigulka}</span></td></tr>')


def lista_tabel(naglowek, nazwy) -> str:
    chipy = "".join(f"<li>{n}</li>" for n in nazwy)
    return (f'<div class="tab-grupa"><h3>{naglowek} — {len(nazwy)} '
            f'{"tabela" if len(nazwy) == 1 else "tabel"}</h3>'
            f'<ul class="tabele">{chipy}</ul></div>')


def zloz(f: Fakty, teraz: datetime) -> str:
    p_em, p_kl, p_sp = f.potoki["emisyjne"], f.potoki["klienci"], f.potoki["sprzedazowe"]
    l = f.liczby

    naglowek = ("Komplet danych dostarczony na czas" if f.wszystko_ok
                else "Część danych jest w trakcie aktualizacji")
    lead = ("Wszystkie źródła zostały przetworzone i są dostępne w Power BI oraz w hurtowni "
            "danych. Raport obejmuje emisje, kampanie, sprzedaż, rezerwacje powierzchni, "
            "wydatki rynkowe oraz bazę klientów."
            if f.wszystko_ok else
            "Poniżej stan na dziś. Obszary oznaczone jako „w trakcie aktualizacji” korzystają "
            "na razie z ostatnich potwierdzonych danych — ich data jest podana przy każdej "
            "pozycji.")

    potoki_html = "\n".join([
        sekcja_potoku("Dane emisyjne",
                      ["Rezerwacje i kampanie", "Statystyki odtworzeń", "Ekrany w metrze"],
                      [("Pobranie", "z 3 systemów"), ("Łączenie", "i wzbogacenie"),
                       ("Model", f"{len(f.tabele['emisyjne'])} tabel")],
                      ["Power BI — raporty", "Hurtownia danych"], "emisyjne",
                      f"{sep(l.get('wierszy_emisji', 0))} wierszy emisji · "
                      f"{sep(l.get('kampanie', 0))} kampanii · codziennie rano",
                      [("Model analityczny", f.tabele["emisyjne"])]),
        sekcja_potoku("Baza klientów",
                      ["CRM — organizacje", "CRM — historia działań"],
                      [("Pobranie", "organizacji i aktywności"),
                       ("Wzbogacenie", "opiekunowie, branże, NIP")],
                      ["SharePoint", "Power BI — raporty"], "klienci",
                      f"{sep(l.get('organizacje', 0))} organizacji i "
                      f"{sep(l.get('aktywnosci', 0))} działań · odświeżane co dwie godziny "
                      f"w dzień roboczy",
                      [("Zestawienie", ["Organizacje z NIP, opiekunami, branżą "
                                        "i historią ostatnich działań"])]),
        sekcja_potoku("Dane sprzedażowe",
                      ["System sprzedażowy", "Rezerwacje powierzchni",
                       "Wydatki rynkowe Kantara", "Pliki planowania i pakietów"],
                      [("Pobranie", "z 4 źródeł"), ("Uzgodnienia", "wartości i klasyfikacji"),
                       ("Model", f"{len(f.tabele['swat_model'])} tabel")],
                      ["Hurtownia danych"], "sprzedazowe",
                      "sprzedaż, budżety, rezerwacje i wydatki rynkowe · codziennie rano",
                      [("Warstwa robocza", f.tabele["swat_robocza"]),
                       ("Model analityczny", f.tabele["swat_model"])]),
    ])

    return SZABLON.format(
        data=po_polsku(teraz), naglowek=naglowek, lead=lead,
        kafle="\n".join([kafel(p_em), kafel(p_kl), kafel(p_sp)]),
        wiersze="\n".join([
            wiersz_dostepnosci("Emisje i realizacja kampanii",
                               "emisje, impresje, realizacja czasowa i ilościowa per kampania, "
                               "panel i dzień", p_em, "Power BI · hurtownia"),
            wiersz_dostepnosci("Baza klientów",
                               "organizacje z NIP, opiekunami i historią ostatnich działań",
                               p_kl, "SharePoint · Power BI"),
            wiersz_dostepnosci("Sprzedaż i budżety",
                               "sprzedaż per kampania, budżety per panel i blok, uzgodnienia "
                               "wartości kontraktów", p_sp, "hurtownia"),
            wiersz_dostepnosci("Rezerwacje powierzchni",
                               "status powierzchni per panel i blok, komplet 24 bloków na rok",
                               p_sp, "hurtownia"),
            wiersz_dostepnosci("Wydatki rynkowe (Kantar)",
                               "wydatki konkurencji per medium, produkt i kreacja",
                               p_sp, "hurtownia"),
        ]),
        emisje=sep(l.get("wierszy_emisji", 0)), kampanie=sep(l.get("kampanie", 0)),
        organizacje=sep(l.get("organizacje", 0)), aktywnosci=sep(l.get("aktywnosci", 0)),
        tabele=l.get("tabel_razem", 0), przyrost=sep(l.get("przyrost_emisji", 0)),
        potoki=potoki_html, wygenerowano=teraz.strftime("%H:%M"),
    )


def sekcja_potoku(tytul, zrodla, etapy, wyniki, ident, skala, grupy) -> str:
    return f"""
  <section>
    <h2>{tytul} — skąd pochodzą</h2>
    <div class="diagram">
{svg_potok(zrodla, etapy, wyniki, f"Przeplyw: {tytul}", ident)}
    </div>
    <p class="cap">{skala}</p>
{chr(10).join(lista_tabel(n, t) for n, t in grupy)}
  </section>"""


SZABLON = """<!doctype html>
<html lang="pl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Raport dzienny danych — {data}</title>
<style>
  *,*::before,*::after {{ box-sizing: border-box; }}
  :root {{
    --paper:#fff; --card:#fff; --ink:#171b1e; --ink-soft:#5a6469;
    --hairline:#dbe0da; --accent:#1c6467;
    --ok:#1f7a4d; --ok-bg:#e6f1ea; --ok-line:#b7d6c3;
    --czeka:#8a5f10; --czeka-bg:#f7eed6; --czeka-line:#e2cf9d;
    --sans: ui-sans-serif, system-ui, "Segoe UI", Roboto, sans-serif;
    --mono: ui-monospace, "Cascadia Mono", Consolas, monospace;
  }}
  body {{ background: var(--paper); color: var(--ink); font-family: var(--sans);
    font-size: 16px; line-height: 1.55; margin: 0; padding: 2.5rem 1.25rem 3rem; }}
  .wrap {{ max-width: 62rem; margin: 0 auto; display: flex; flex-direction: column; gap: 2rem; }}
  .eyebrow {{ font-family: var(--mono); font-size: .75rem; letter-spacing: .14em;
    text-transform: uppercase; color: var(--accent); }}
  h1 {{ font-size: clamp(1.7rem, 4vw, 2.3rem); line-height: 1.15; letter-spacing: -.02em;
    margin: .35rem 0 .5rem; text-wrap: balance; }}
  h2 {{ font-size: 1.05rem; margin: 0 0 .85rem; padding-bottom: .5rem;
    border-bottom: 1px solid var(--hairline); }}
  p {{ margin: 0 0 .75rem; max-width: 65ch; }}
  .lede {{ color: var(--ink-soft); }}

  .verdicts {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: .8rem; }}
  .verdict {{ background: var(--card); border: 1px solid var(--hairline);
    border-left: 4px solid var(--ok); border-radius: 3px; padding: .9rem 1rem;
    display: flex; flex-direction: column; gap: .15rem; }}
  .verdict.czeka {{ border-left-color: var(--czeka); }}
  .verdict .name {{ font-family: var(--mono); font-size: .72rem; letter-spacing: .07em;
    text-transform: uppercase; color: var(--ink-soft); }}
  .verdict .score {{ font-size: 1.3rem; font-weight: 650; color: var(--ok); }}
  .verdict.czeka .score {{ font-size: 1.05rem; color: var(--czeka); }}
  .verdict .when {{ font-size: .82rem; color: var(--ink-soft); font-variant-numeric: tabular-nums; }}
  @media (max-width: 40rem) {{ .verdicts {{ grid-template-columns: 1fr; }} }}

  .scroll {{ overflow-x: auto; border: 1px solid var(--hairline); border-radius: 3px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: .9rem; }}
  th, td {{ text-align: left; padding: .6rem .8rem; border-bottom: 1px solid var(--hairline);
    vertical-align: top; }}
  th {{ font-family: var(--mono); font-size: .7rem; letter-spacing: .1em; text-transform: uppercase;
    color: var(--ink-soft); font-weight: 500; white-space: nowrap; }}
  tr:last-child td {{ border-bottom: none; }}
  td.when {{ font-family: var(--mono); font-variant-numeric: tabular-nums; white-space: nowrap; }}
  .pill {{ display: inline-block; font-family: var(--mono); font-size: .7rem; font-weight: 600;
    letter-spacing: .06em; text-transform: uppercase; padding: .15rem .45rem; border-radius: 2px;
    white-space: nowrap; }}
  .pill.ok {{ color: var(--ok); background: var(--ok-bg); }}
  .pill.czeka {{ color: var(--czeka); background: var(--czeka-bg); }}

  .kpi {{ display: grid; grid-template-columns: repeat(6, 1fr); gap: .5rem; }}
  .kpi .box {{ border: 1px solid var(--ok-line); background: var(--ok-bg); border-radius: 3px;
    padding: .7rem .6rem; display: flex; flex-direction: column; gap: .1rem; }}
  .kpi .v {{ font-size: 1.1rem; font-weight: 650; font-variant-numeric: tabular-nums;
    color: var(--ink); white-space: nowrap; }}
  .kpi .k {{ font-size: .72rem; line-height: 1.3; color: var(--ink-soft); }}
  @media (max-width: 62rem) {{ .kpi {{ grid-template-columns: repeat(3, 1fr); }} }}

  .diagram {{ background: #fbfcfa; border: 1px solid var(--hairline); border-radius: 3px;
    padding: 1rem; overflow-x: auto; }}
  svg {{ display: block; max-width: 100%; height: auto; }}
  .cap {{ font-size: .82rem; color: var(--ink-soft); margin: .5rem 0 0; max-width: 72ch; }}

  .tab-grupa {{ margin-top: .9rem; }}
  .tab-grupa h3 {{ font-family: var(--mono); font-size: .7rem; letter-spacing: .1em;
    text-transform: uppercase; color: var(--accent); font-weight: 600; margin: 0 0 .4rem; }}
  ul.tabele {{ list-style: none; margin: 0; padding: 0; display: grid;
    grid-template-columns: repeat(4, 1fr); gap: .3rem; }}
  ul.tabele li {{ font-size: .76rem; line-height: 1.25; background: var(--ok-bg);
    border: 1px solid var(--ok-line); border-radius: 2px; padding: .3rem .45rem; }}
  @media (max-width: 48rem) {{ ul.tabele {{ grid-template-columns: repeat(2, 1fr); }} }}

  footer {{ border-top: 1px solid var(--hairline); padding-top: 1rem; font-size: .82rem;
    color: var(--ink-soft); }}

  @media print {{
    @page {{ size: A4; margin: 14mm; }}
    * {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
    body {{ padding: 0; font-size: 10.5pt; line-height: 1.45; }}
    .wrap {{ max-width: none; gap: 1.2rem; }}
    h1 {{ font-size: 20pt; }} h2 {{ font-size: 12pt; break-after: avoid; }}
    section, .diagram, .scroll, table, .verdicts, .kpi, .tab-grupa {{ break-inside: avoid; }}
    tr {{ break-inside: avoid; }}
    /* A4 z marginesami to ~43rem, wiec breakpointy `max-width` wyzej ZLAPALYBY sie
       w druku i zlamaly siatki mimo poprawnych regul bazowych. Przywracamy jawnie. */
    .kpi {{ grid-template-columns: repeat(6, 1fr); }}
    ul.tabele {{ grid-template-columns: repeat(4, 1fr); }}
    .kpi .v {{ font-size: 11pt; }} .kpi .k {{ font-size: 7.5pt; }}
    ul.tabele li {{ font-size: 7.5pt; padding: .22rem .35rem; }}
  }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="eyebrow">Raport dzienny · {data}</div>
    <h1>{naglowek}</h1>
    <p class="lede">{lead}</p>
  </header>

  <section class="verdicts">
{kafle}
  </section>

  <section>
    <h2>Co jest dostępne</h2>
    <div class="scroll"><table>
      <thead><tr><th>Obszar</th><th>Zakres</th><th>Aktualność</th><th>Dostępne w</th><th>Stan</th></tr></thead>
      <tbody>
{wiersze}
      </tbody>
    </table></div>
    <p class="cap">Dane odświeżane są codziennie rano; baza klientów aktualizuje się co dwie godziny w ciągu dnia roboczego.</p>
  </section>

  <section>
    <h2>Skala przetworzonych danych</h2>
    <div class="kpi">
      <div class="box"><span class="v">{emisje}</span><span class="k">wierszy emisji</span></div>
      <div class="box"><span class="v">{kampanie}</span><span class="k">kampanii w modelu</span></div>
      <div class="box"><span class="v">{organizacje}</span><span class="k">organizacji w bazie</span></div>
      <div class="box"><span class="v">{aktywnosci}</span><span class="k">działań handlowych</span></div>
      <div class="box"><span class="v">{tabele}</span><span class="k">tabel w hurtowni</span></div>
      <div class="box"><span class="v">{przyrost}</span><span class="k">nowych emisji z doby</span></div>
    </div>
    <p class="cap">Każdej doby przetwarzanych jest 9 niezależnych źródeł zasilających {tabele} tabel — ich pełny wykaz znajduje się przy schematach poniżej.</p>
  </section>
{potoki}

  <footer>
    Raport wygenerowany {data} o godz. {wygenerowano}. Kolejne odświeżenie danych: następnego dnia rano.
  </footer>
</div>
</body>
</html>
"""
