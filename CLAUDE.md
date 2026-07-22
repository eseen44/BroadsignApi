# BroadsignApi — kontekst projektu

> Aktualizacja: 2026-07-16. Jeśli coś tu przeczy kodowi — **kod wygrywa**, a ten plik zaktualizuj.
> Dodatkowa pamięć operacyjna (gotchy, stan WIP): `C:\Users\janr\.claude\projects\C--Projects-BroadsignApi\memory\` (indeks: `MEMORY.md`).

## Co to jest
Pipeline danych Broadsign → Power BI dla Ströer Polska. Użytkownik: jratajski@stroeer.pl (Admin).
Bronze (fetch z API → parquet) → Silver (join/wzbogacenie) → Gold (star schema) → OneDrive/SharePoint → PBI.

## Python
- Lokalnie (Windows): `C:\ProgramData\Anaconda3\python.exe` — używać tej ścieżki, NIE `python`/`py`.
- Na VM (`broadsign00`): zwykłe `python3`.

## Uruchamianie pipeline
```
cd C:\Projects\BroadsignApi
"C:\ProgramData\Anaconda3\python.exe" run_pipeline.py       # pełny pipeline lokalnie
```
Na VM produkcyjnie: `/dane/BroadsignApi/run_pipeline.sh` (cron `30 9 * * *`), Bronze→Silver→Gold→sync. Log: `/dane/BroadsignApi/pipeline.log`.
**Uwaga:** `run_pipeline.py` leci przez wszystkie warstwy niezależnie od FAIL wcześniejszej (agreguje tylko boolean na końcu) — może cicho wypchnąć niekompletne dane. Do świadomości przy debugowaniu.

## 🔁 Workflow wdrożenia zmiany w Gold (KLUCZOWE — powtarza się co chwilę)
Zmiana w `Pipeline/gold/*` → do PBI:
1. Edytuj plik, `git add` + `git commit` (na branchu `main`, w **głównym checkoucie** `C:\Projects\BroadsignApi` — NIE w worktree; `deploy_to_vm.sh` robi `git archive HEAD`, więc worktree wysłałby zły commit).
2. `./deploy_to_vm.sh` — wysyła gitowane pliki na VM (git archive + ssh tar, konwersja CRLF→LF).
3. Na VM przelicz dotknięte tabele: `ssh ... "cd /dane/BroadsignApi && python3 -m Pipeline.gold.build_XXX"`.
4. Skopiuj parquety do OneDrive: `cp -f Data/gold/*.parquet /dane/OneDrive/Pulpit/UbuntuSynch/`.
5. Wypchnij na SharePoint: `/usr/bin/onedrive --synchronize --single-directory 'Pulpit/UbuntuSynch' --verbose`.
6. Odśwież w PBI (patrz niżej — **cache!**).

**Gotchy VM/OneDrive (potwierdzone wielokrotnie):**
- Demon onedrive na VM jest `inactive` — sync trzeba odpalać ręcznie (albo czeka na cron `*/15`).
- `onedrive --synchronize` re-uploaduje plik **tylko gdy zmienił się hash treści** (sam `touch`/nowy mtime nie wystarczy). `save_gold` daje nowy `_gold_at` przy każdym buildzie → hash się zmienia → OK.
- Weryfikacja co realnie poszło na SharePoint: czytaj lokalny mirror usera
  `C:\Users\janr\OneDrive - Stroeer Poland Sp. z o.o\Pulpit\UbuntuSynch\*.parquet` (kolumna `_gold_at`).

## ⚠️ CACHE SharePoint.Files w Power BI (największa, powtarzalna pułapka)
Power Query cache'uje odpowiedzi `SharePoint.Files`. **Refresh przez XMLA / PowerBI MCP (`partition_operations Refresh`) NIE busta tego cache'u** — pokazuje sukces, ale ładuje starą wersję pliku. Objaw: `_gold_at` w modelu ≠ `_gold_at` na dysku usera mimo "udanego" refreshu.
**Jedyne co działa:** user robi **Narzędzia główne → Odśwież** w Desktopie (pełny UI refresh), a jak i to nie pomoże — **zamknij i otwórz** `.pbip`. Zawsze proś usera o to i weryfikuj `_gold_at` po fakcie. Nie da się tego zrobić za usera przez MCP.

## Autentykacja (wszystko w `.env`, gitignored)
- **Direct API**: cookie (`POST /login`) — `BROADSIGN_EMAIL`, `BROADSIGN_PASSWORD` (`Package/auth.py`, `get_session()`)
- **Control API**: bearer, `https://api.broadsign.com:10889/rest/` — `BROADSIGN_CONTROL_API_KEY`
- **Popstats**: `https://popstats.broadsign.com/stroer_polska/` — rolling window ~2 mies. (starsze pliki znikają bezpowrotnie)
- **MagicInfo**: TOTP — `MI_USER`, `MI_PASSWORD`, `MI_TOTP_SECRET`. Rolling window ~31 dni. `_upsert_month` naprawiony na upsert po kluczu `(format,content_id,play_date)` — NIE używać `--force` z logiką delete-then-insert (skasowała dane 2026-07-13, patrz [[magicinfo_integration]]).

## Struktura projektu
```
Package/            # klienci API: auth, direct/, control/client, popstats/client, magicinfo/, direct/reporting.py
Pipeline/
├── bronze/         # fetch → parquet. utils.py: save_parquet(overwrite, guard na pusty df),
│                   #   upsert_parquet(po id), append_parquet, upsert_by_date
├── silver/         # join+wzbogacenie (build_campaigns, build_play_logs, build_players, build_magicinfo_pop...)
├── gold/           # star schema:
│   ├── build_dim_campaign.py        # AKTYWNY (build_dim_campaign_full.py = MARTWY, nieużywany)
│   ├── build_dim_line_item.py       # + kolumna broadsign_status (Direct API performance.status)
│   ├── build_dims.py                # dim_date/screen/player/content
│   ├── build_fact_play_logs.py      # emisje/Impresje/Duration, is_serwisowy per wiersz
│   ├── build_fact_budget.py         # koszt+repetycje per lineitem×player×dzień; korekta LL/StroerTV
│   ├── build_fact_health.py         # panel×dzień incydenty
│   ├── build_dim_campaign_period.py # bridge miesiąc×kampania dla slicera
│   ├── run_all.py                   # orkiestrator (STEPS)
│   └── utils.py                     # zbiory wykluczeń + get_single_panel_campaign_ids()
Data/gold/          # parquety dla PBI (sync do OneDrive/UbuntuSynch)
```

## Gold — logika wykluczeń / flag (Pipeline/gold/utils.py)
Trzy poziomy, WSZYSTKIE stosowane spójnie w builderach:
- **`is_serwisowy`** (flaga, dane ZOSTAJĄ — user filtruje w raporcie):
  - `=1` serwisowe/autopromo/systemowe. Źródła: `SERWISOWY_CAMPAIGN_IDS` (po campaign_id), `SERWISOWY_RESERVATION_IDS` (po reservation_id), `SERWISOWY_LINE_ITEM_IDS` (po line_item_id — dla osieroconych pozycji bez campaign_id, np. `TVP45s`, `Kino Letnie STV`).
  - `=2` single-panel/testowe: `get_single_panel_campaign_ids()` (dokładnie 1 panel) + `FORCE_SINGLE_PANEL_CAMPAIGN_IDS` (ręcznie, gdy brak play_logs).
  - Ustawiane w `build_dim_campaign.py` (poziom kampanii) I `build_fact_play_logs.py` (poziom wiersza).
- **`EXCLUDED_CAMPAIGN_IDS`** — kampanie znikają CAŁKOWICIE z dim_line_item/fact_budget (stare/zepsute, np. Sokoliki 2472239). To co innego niż is_serwisowy.

## Gold — korekta LiveLine/StroerTV w build_fact_budget.py
Broadsign symuluje całą sieć metra LiveLine/StroerTV jako 1-2 "wirtualne" playery → `perf_actual/expected_repetitions` z Direct API są oderwane od skali. Korekta (per-ROW, nie per-line-item):
- Format bierzemy z **playera** (`player_name` → liveline/stroertv), a gdy brak dopasowanego playera → z nazwy line itemu (jednoznaczne) lub blended split 68/32 (bundle/MetroMax — MetroMax to pakiet mieszający oba formaty pod jednym LI).
- `actual` skalowany stałą `ACT_CORRECTION` (kalibrowana vs MagicInfo), `expected` liczony OD NOWA ze skorygowanego actual (factor deterministyczny per line_item) → % realizacji ląduje w [1.01, 1.13].
- Bug dzielenia przez `n_players` (dawniej "najważniejszy otwarty") — **NAPRAWIONY** (35f5550). Nie odtwarzać.

## Gold — realizacja CZASOWA: duration z repetycji (build_fact_budget.py) — wariant A, 2026-07-17
**Duration = emisje wyrażone w sekundach**, na TEJ SAMEJ podstawie co `Health % (emisje)`. Liczone w `fact_campaign_budget` (NIE ma już osobnej tabeli fact_fill — usunięta):
- `duration_expected_sec = daily_expected_repetitions × slot_duration`
- `duration_actual_sec  = daily_actual_repetitions  × slot_duration`
- Repetycje to skorygowane expected/actual Broadsign (z korektą LL/StroerTV, n_players) → Duration i emisje mają jeden mianownik → **spójne z definicji**. `Duration % = emisje % przeważone długością slotu` (30s liczy się 2× bardziej niż 15s). `slot_duration` z line itemu, brak → fallback 15s (issue #14). **Zweryfikowano 2026-07-21: fallback nigdy się nie uruchamia** — 0/3590 line itemów ma puste `slot_duration` w produkcji; rozkład wartości: 15s (3356, 93.5%), 10s (206), 30s (19), reszta marginalna. Fallback 15s i tak pokrywałby się z dominującą wartością gdyby był potrzebny — nic do kalibracji.
- **Dlaczego przeprojektowane:** wcześniejszy `fact_fill` miał WŁASNY mianownik (`active_hours × skalibrowane EMISJE_PER_HOUR`) niezależny od emisje % → dwa różne "100%", niespójne. Kalibracja pętli per format (180/90/120s) była krucha. Wariant A wywala to wszystko. Problem peak-buyów znika za darmo (Broadsign expected zna realny harmonogram zakupu).
- Zwalidowane VM 2026-07-17: **Duration % 107.1% ≈ emisje % 107.3%**.
- Miary PBI (`_Miary`, folder Health): `Duration dostarczony` = `SUM(fact_campaign_budget[duration_actual_sec])`, `Duration oczekiwany` = `SUM(fact_campaign_budget[duration_expected_sec])`, `Duration %`, `Duration OK` (3% luzu vs Upływ czasu %). Kolumny `duration_*_sec` w modelu ukryte, `summarizeBy=none`.

## Power BI — plik, narzędzia
**Plik kanoniczny:** `C:\Users\janr\OneDrive - Stroeer Poland Sp. z o.o\Pulpit\PBI\BroadSignApi\BroadsignApi2.pbip` (format PBIP: `.Report` + `.SemanticModel`). Stare `.pbix` na Pulpicie — NIE używać.

**Dwa kanały:**
1. **`pbir` CLI** (skill `pbir-cli`) — struktura raportu: strony, wizualy, tła SVG, pozycje, filtry, slicery.
   - `pbir desktop list` (sprawdź czy otwarty), `pbir tree "<SNAP>/Str.Page"`, `pbir get ... --json`, `pbir visuals bind/position`, `pbir set "<...>.prop" --value`, `pbir cp` (kopiuj wizual), `pbir validate --fields`.
   - `SNAP` = **pełna ścieżka** do `...BroadsignApi2.Report` (nie sama nazwa — przy duplikatach nazw pomyli pliki).
   - `pbir desktop refresh` często rzuca fałszywy "no running instance" — ponów raz.
2. **PowerBI MCP** (`mcp__powerbi__*`, deferred — doładuj przez ToolSearch) — model: miary, relacje, kolumny, partycje.
   - `connection_operations ListLocalInstances` → `Connect` (połączenie pada po timeoutach — przełącz się ponownie).
   - `dax_query_operations Execute` (debug na żywo), `measure_operations Update/Create`, `column_operations`, `partition_operations`, `relationship_operations`, `model_operations Refresh (Calculate)` (po dodaniu relacji/kolumn obliczanych).
   - **Zmiany przez MCP żyją TYLKO w pamięci Desktopa do Ctrl+S.** Nowe kolumny obliczane wymagają `model_operations Refresh Calculate`; nowe kolumny danych wymagają odświeżenia danych (i patrz: cache SharePoint wyżej).

## Architektura modelu (relacje)
```
dim_date ──┐
dim_campaign (1) ─► dim_lineitem (*) ─► fact_campaign_budget / fact_play_logs (*)
dim_player ─┤
dim_content ┘
dim_campaign_period ◄─BothDirections─► dim_campaign  (bridge miesiąc×kampania: slicer WYBIERA kampanie, nie filtruje ich playlogów)
```
- `dim_campaign → dim_lineitem` (OneDirection) — jedyny snowflake hop, dodany celowo (filtr kampanii → pola dim_lineitem). NIE cofać.
- Skutek: usunięto bezpośrednie `fact_* → dim_campaign`. **Błąd "CROSSFILTER can only use columns participating in a relationship"** → napraw podwójnym CROSSFILTER przez dim_lineitem jako most (wzorzec w miarze `Distinct advertisers`).

## Miary — mapa (displayFolder w `_Miary`)
- **Health**: `Dni/Panele zaplanowane/z emisjami/%/OK`, `Pokrycie dni %`, `Upływ czasu %`, `Health % (emisje)`, `Health emisje - licznik/mianownik`, `Dni OK (emisje)`, `Kampanie OK/Problem`, `Impresje oczekiwane/dostarczone`, oraz **Duration**: `Duration dostarczony/oczekiwany/%/OK`, `Aktywne godziny`. Miary "OK"/status używają `[Upływ czasu %]` + 5% luzu (tolerancja 0.95, poszerzone z 3%/0.97 — 2026-07-22, user decyzja) — dotyczy `_Health Status`, `_Health Status (Emisje)`, `Panele OK`, `Dni OK (emisje)`, `Duration OK`, `dim_campaign[Health OK]` (wszystkie spójnie). Sentinel "brak danych" = `-1` (NIE `BLANK()`, bo `BLANK()=0` daje TRUE w DAX).
- **Emisje**: `Fill Rate (play-log)` / `AutoPromo` (denominator = emisje komercyjne+autopromo, po Duration i po liczbie — REMOVEFILTERS na obu is_serwisowy), `Fill Rate/AutoPromo (Duration)`, `Fill Rate + AutoPromo`, `Sloty zajęte/max`, `Distinct advertisers/campaigns`.
- **Revenue**: `Przychód` + warianty, `Rabat % YTD`.
- **SVG Cards**: `KPI ... SVG` (Health, Kampanie, Przychód, Rabat, Statystyki, Proof, Planowane), `Heatmap ... SVG`.
- **dim_campaign**: kolumna obliczana `Health OK` (✅/⚠️/Brak danych, do slicera). **dim_lineitem**: `broadsign_status`, `Realizacja %`, `Anomalia Realizacji` (>250% = wada danych źródłowych, do filtrowania).

## Znane bugi DAX — stan
NAPRAWIONE: `Emisje kumulatywnie` (ALLSELECTED(dim_date)), `Fill Rate` (przerobiony na fill+autopromo, spójne mianowniki), `Przychód cennik (timeline)` (2026-07-16 — dawny odwrócony ratio zastąpiony prostym `SUM(daily_cost_line)`; siostrzana `Przychód (timeline)` ma osobny, poprawny ratio TOTAL/Subtotal; wpis o buggu w tej sekcji był nieaktualny, zweryfikowano 2026-07-21).
**`Przychód (timeline) MTD`/`LMTD` (naprawione 2026-07-21):** obie liczyły do `EOMONTH(...)` (koniec całego miesiąca, WŁĄCZNIE z przyszłymi dniami — `fact_campaign_budget` ma zaplanowane wiersze na przyszłość) zamiast do dziś. MTD zawyżało o +32,5% (3 277 350 zamiast 2 210 690). Poprawione na `<= TODAY()` (MTD) i `<= EDATE(TODAY(),-12)` (LMTD, dzień-w-dzień rok wstecz). `Przychód cennik (timeline) MTD` już wcześniej robił to poprawnie (`<= TODAY()`) — stąd rozjazd. `MTD vs LMTD %` (zależna miara) nietknięta, przelicza się poprawnie z nowych bazowych.
**`Przychód SPLY`/`Przychód ACT` (naprawione 2026-07-21):** filtrowały `dim_date[Year]=X` bez `ALL(dim_date)` — pod jakimkolwiek innym filtrem na `dim_date[Date]` (np. istniejący filtr strony Revenue "Date > 31.05") SPLY zapadał się do BLANK (rok 2025 nie może spełnić "Date > maj 2026"), a ACT liczył tylko wycinek roku. Dodano `ALL(dim_date)` do obu — teraz odporne na filtr strony, zweryfikowane: SPLY=81,96M, ACT=134,01M niezależnie od filtra.
**`Czas emisji (h)` — też już naprawione (2026-07-15, dzień przed "Aktualizacją"), wpis był nieaktualny.** Miara dzieli przez 3600 (nie 3600000). Zweryfikowano 2026-07-21: implikowana średnia długość spotu = `Czas emisji(h)×3600/Emisje` = 14,9s — pasuje do znanego fallbacku 15s.
WCIĄŻ OTWARTE (z audytu, potwierdzone): brak — wszystkie 5 pozycji z tej sekcji sprawdzone 2026-07-21; 2 były już naprawione ale nieodhaczone, 1 (Rabat % YTD) to zamierzone zachowanie (probono), 2 (Przychód MTD/LMTD, Przychód SPLY/ACT) realnie naprawione dziś.

**`Rabat % YTD` — NIE bug, zweryfikowane 2026-07-21.** Kampanie z `campaign_price=0` (probono, celowo) dają ratio=0 w `Przychód (timeline)` (licznik), więc ich cała wartość Sub total liczy się jako 100% rabatu — to zamierzone, user potwierdził że efektywny rabat 54% (YTD, is_serwisowy=0) jest realny (~20% bazy to probono). Zdjęte z listy otwartych bugów.

**NAPRAWIONE (2026-07-22): Pivot matrix — `Cena`/`Rabat` pokazywały identyczną wartość na każdym wierszu przy grupowaniu po polu z `dim_date` (Rok/Miesiąc/Data z `SelectorPivot`).** Przyczyna: `Cena`=`Przychód (timeline) YTD` i `Rabat`=`Rabat % YTD` (przez `Przychód cennik (timeline) YTD`) mają `ALL(dim_date)` + sztywny zakres "od 1 stycznia do dziś" — to kasowało dokładnie ten filtr, który miał różnicować wiersze. Potwierdzone na żywo: grupowanie po Roku dawało Cena=Rabat identyczne dla 2025/2026/2027 (30 541 867 / 67,55% wszędzie), podczas gdy Cennik/Emisje poprawnie się różniły. Naprawa: nowa miara `Rabat % (okres)` (bez ALL, respektuje kontekst) + w Pivot matrix (`f9dac34e9af12c87ffe2`) podpięto Cena→`Przychód (timeline)` (już istniejąca, bezYTD wersja) i Rabat→`Rabat % (okres)` — pod tymi samymi etykietami. `Rabat % YTD`/`Przychód (timeline) YTD` NIE ruszone (używane przez `KPI Rabat SVG` na Revenue, muszą zostać YTD). Zweryfikowane: po fixie Rok 2025/2026/2027 dają Cena=55,8M/36,4M/0,5M, Rabat=38,4%/72,8%/99,1% (poprawnie różne). PlayLog (`FieldsSelector`) sprawdzony analogicznie — logicznie czysty, wszystkie miary Values (fact_play_logs-based) poprawnie filtrowane przez wszystkie 27 pól, brak ALL()/REMOVEFILTERS.

**NOWE (2026-07-21): `dim_campaign[Health OK]` vs `Kampanie OK`/`Kampanie Problem` — dwa różne okna czasowe.**
`Health OK` (kolumna obliczana na `dim_campaign`, używana w slicerze) liczy z CAŁEJ historii — kolumny obliczane nie widzą filtrów raportu/strony. `Kampanie OK`/`Kampanie Problem` (miary za kartą SVG `KPI Health SVG`) liczą przez miarę `_Health Status (Emisje)`, która W PEŁNI respektuje filtry strony (np. `Date is after X`). Efekt: slicer klasyfikuje kampanię jako "Problem" wg całej historii, a karta/tabela w wybranym oknie czasowym może pokazywać tę samą kampanię jako OK (i odwrotnie). Zmierzone na żywo: z filtrem `dim_campaign_period[year]=2026` obie strony się zgadzają (335/55 = 335/55), ale po dodaniu filtru daty (`dim_date[Date]>31.05`) miara spada do 114/27 podczas gdy kolumna zostaje przy 335/55 — potwierdzony rozjazd. User naprawia to przez ujednolicenie filtrów daty w Power Query per-tabelowo (przycięcie źródła do tego samego okna wszędzie) zamiast zmiany DAX — patrz też uwaga o `dim_campaign_period` niżej (grain M+W wymaga OSOBNEGO traktowania kolumny `year_month_dt` i `year_week_dt` w każdym takim filtrze, inaczej ginie cały grain W).

## Strony raportu
Revenue, Health, FillRate, PlayLog, Proof. `PlayLog` sterowany parametrem `FieldsSelector` (multi-select field parameter — grupowanie zależy od zaznaczeń, nie stała struktura). Strony Revenue/Health mają własny filtr `dim_campaign[is_serwisowy]=0` — dlatego miary muszą mieć `REMOVEFILTERS(dim_campaign[is_serwisowy])` gdzie liczą całość.

## Otwarte usprawnienia (nie bugi, redesign)
**FillRate page — heatmapa przeprojektowana (2026-07-21, NAPRAWIONE).** Realny wizual to Deneb/raw-Vega ("Advertisers per Date, Timeslot" → przemianowany na "Campaigns per Date, Timeslot") — NIE miary `Heatmap Advertisers/Campaigns SVG` w `_Miary` (te są martwe/niepodpięte, zostały jako relikt, nie usuwane). Prawdziwy problem nie był w typie wizuala tylko w wykonaniu: brak limitu zakresu dat (oś X brała wszystkie daty przechodzące przez filtr strony → setki cienkich kolumn przy szerokim filtrze roku) i brak liczb w komórkach (tylko kolor + tooltip po hover). Naprawione w `visual.json` (Vega spec): dodano transform ograniczający do ostatnich 14 dni (`window row_number <= 14` po dacie), dodano warstwę `text` z liczbą w każdej niepustej komórce (kontrastowy kolor tekstu wg progu `value/max > 0.7`), zmieniono miarę z `Distinct advertisers` na `Distinct campaigns` (konwencja campaign_id, spójna z resztą modelu), zerowe komórki dostają neutralny szary zamiast najciemniejszego fioletu viridis. Paleta viridis zostawiona (sekwencyjna, dobra teoria koloru) — problem był w rozmiarze/braku liczb, nie w kolorach. Zweryfikowane zrzutem ekranu z żywego Desktopa.

## VM produkcyjny — broadsign00
- SSH: `ssh -i /c/Users/janr/.ssh/id_ed25519 janr@10.1.2.19` (Bash wymaga jawnej ścieżki klucza). Wymaga VPN — timeout ssh to prawie zawsze VPN, nie VM. Brak sudo (pip `--user --break-system-packages`).
- `/dane/BroadsignApi/` = repo (deploy przez `deploy_to_vm.sh`, tylko gitowane pliki — NIE `.env`/`Data/`). Brak `git` na VM.
- `/dane/Broadsign_Logs/ScriptLinux.py` — osobny, ręcznie utrzymywany, DRYFUJE od repo (`ScriptLinux_v4.py`). Nie nadpisywać bez diffu.
- Cron (janr): `*/15` onedrive sync, `0 6` Pipedrive, `0 9` ScriptLinux.py, `30 9` run_pipeline.sh.

## Broadsign reporting endpoints — do dalszego zbadania
`Package/direct/reporting.py`: `fill_rate_breakdown` (live, max miesiąc wstecz, `fill_pressure` per line item) i `screen_allocation` (`POST /reporting/screen_allocation {storage:{type:BROADSIGN}}` → presigned URL .csv.gz, async, **rate-limit 429**). `screen_allocation` daje zaplanowany udział w pętli (`proposal_item_pressure`) per line item per ekran, miesiąc wstecz + 18mc w przód — potencjalne autorytatywne źródło "obłożenia/fill". Zwalidowane dla ekranów fizycznych (StroerTV wirtualny 1:1), ale **granularność ekranu kupowalnego ≠ fizycznego panelu** (play_logs) → wymaga pogodzenia zanim się użyje. Odłożone. Do live-TV zajętości dla handlowców (prośba Michała/Sandera).

## Odzysk danych / gotchy
- Popstats rolling window ~2 mies. Odzyskano historię z backupu `playlog_backup.tar.gz` do `Data/bronze/play_logs.parquet`. Nieodzyskiwalna dziura: 2025-04-08..2025-06-25.
- Przy odzysku z `tar.gz`: iteruj `for m in tar:` w kolejności strumienia (r:gz jednokierunkowy — losowy dostęp = O(n²)).
- **`resources_backup.tar.gz`** (obok `playlog_backup.tar.gz` na Desktopie, `C:\Users\janr\Desktop\BroadsignBackup\`, ten sam okres 2025-06-27..2026-06-08) **NIE jest jeszcze wgrany do pipeline'u** — istnieje tylko `fetch_resources_latest()` (upsert bieżącego dnia), brak historycznego backfillu. Zawiera `id/name/flag/type` (w tym `type=reservation`) — potencjalne źródło nazw dla ID, które nie odzyskają się przez Direct API (patrz niżej).

**NAPRAWIONE (2026-07-22): `fact_play_logs` — 31 osieroconych `line_item_id` bez `campaign_name` (blank w Pivot/PlayLog).** Root cause: `Package/direct/proposal_items.py::search_proposal_items()` nie miała `$sort` (w odróżnieniu od `search_proposals()`), więc niedeterministyczna paginacja `$skip`/`$top` gubiła rekordy między stronami przy pełnym fetchu (~40 stron dla ~4000 itemów) — dokładnie ten sam mechanizm co w `c5c1b6f` (28.05.2026), nigdy do końca nie naprawiony. Zweryfikowano GET-by-ID (`get_proposal_item`) na żywym Direct API: **31/31 line itemów istnieje** (żaden nie usunięty z Broadsign), w tym `TVP45s` i `Kino Letnie (15.V-31.VIII) STV` — te dwa mają teraz realny `campaign_id` (2464051/2527383), więc wpis w `SERWISOWY_LINE_ITEM_IDS` uzasadniony "brakiem campaign_id" jest już nieaktualny dla nich (flaga `is_serwisowy=1` może zostać, samo uzasadnienie nie).
- Fix: dodano `$sort` do `search_proposal_items()`.
- **Self-healing**: nowa funkcja `Pipeline/bronze/fetch_direct.py::reconcile_missing_proposal_items()` — po każdym `fetch_proposal_items()` porównuje `line_item_id` z `fact_play_logs` (gold, poprzedni przebieg) vs bronze `proposal_items`, brakujące dociąga pojedynczo przez `get_proposal_item`/`get_proposal` (działa nawet gdy search ich nie widzi) i upsertuje — wpięte jako krok `reconcile_proposal_items` w `Pipeline/bronze/run_all.py`, więc kolejny silver/gold w TYM SAMYM cyklu pipeline'u już to widzi. Przyszłe podobne przypadki powinny łatać się same.
- Backfill wykonany 2026-07-22 na VM: 31/31 line itemów + 22 proposals odzyskane, silver+gold przebudowane, zsynchronizowane do SharePoint. **Wymaga pełnego "Odśwież" w Power BI Desktop** (cache SharePoint.Files — MCP/XMLA refresh partycji tego nie wymusza, zweryfikowane).
- **Osobna sprawa, częściowo naprawiona**: ~170k wierszy `fact_play_logs` ma `line_item_id = NULL` już w źródle Control API (nie ID którego nie da się dociągnąć przez GET-by-ID — po prostu nigdy go nie było, `proposal_id`/`contract_id`/`line_item_id` wszystkie puste natywnie w `ctrl_reservations_v22`). To tylko **20 unikalnych `reservation_id`**, skrajnie skoncentrowane: **`1125116202` = 96,6% (164 678/170 411 wierszy)**, jeden content ("Uwaga Pociąg" — czujka na stacji przerywa pętlę komunikatem o nadjeżdżającym pociągu, czas kontraktowo należy do Metra Warszawskiego, 2025-06-26..dziś).
  - **`MANUAL_RESERVATION_OVERRIDES`** (nowy słownik w `Pipeline/gold/utils.py`) — dla rezerwacji bez ŻADNEGO bookingu w Broadsign: nadaje syntetyczny `campaign_id`/`line_item_id` z puli `900000000+` (poza zakresem prawdziwych ID) + ręczną nazwę. Wpięte w `build_fact_play_logs.py` (uzupełnia `campaign_id`/`line_item_id` po `res_camp` merge) i `build_dim_campaign.py`/`build_dim_line_item.py` (dokładają syntetyczny wiersz z tą nazwą, żeby join dawał realny `campaign_name` zamiast blank). `900000001` dopisany też do `SERWISOWY_CAMPAIGN_IDS`.
  - Obecnie tylko `1125116202` → "Uwaga Pociąg" / advertiser "Metro Warszawskie". `1230744166` ("test_IT_synchro") już był w `SERWISOWY_RESERVATION_IDS` wcześniej (is_serwisowy=1), ale bez nazwy — nie dodano do `MANUAL_RESERVATION_OVERRIDES`, zostaje blank.
  - **Pozostałe 19 rezerwacji (5733 wiersze, 3,4%) — NAPRAWIONE 2026-07-22.** User zdecydował: wszystkie 19 ręcznie oznaczone `campaign_name="Autopromocja"`, `advertiser/client="Stroer"`, `line_item_name="Autopromocja"` — ściśle po tych konkretnych `reservation_id`, NIE jako ogólna reguła dla wszystkiego nieprzypisanego. Wspólny synthetic `campaign_id=900000002` (jedna kampania), unikalny `line_item_id` 900000003..900000021 per rezerwacja (inaczej `dim_line_item` dostałby duplikat klucza). `build_dim_campaign.py` dedupikuje syntetyczne wiersze po `campaign_id`. Uwaga: `1987096469` (Amazon branding, Wagony) wyglądał na realny gap komercyjny nie autopromo — user mimo to zdecydował objąć go tą samą regułą.
  - **Wynik: `NULL campaign_id` w `fact_play_logs` = 0** (było 170 411). Zdeployowane, gold przebudowany na VM, zsynchronizowane do SharePoint.

## Bezpieczeństwo — stan zweryfikowany 2026-07-21
1. **Hasło popstats (BroadsignApi)**: historia gita wyczyszczona BFG-iem 2026-05-28 (`..bfg-report/2026-05-28/`, przepisał 3 commity `client.py`). Remote URL dziś czysty (bez PAT). **Niepotwierdzone: czy samo hasło zostało też zrotowane u źródła** (BFG czyści tylko historię, nie unieważnia credentiala) — do potwierdzenia z userem.
2. **🚨 AKTYWNE, PILNE: PAT w remote URL repo Pipedrive.** `C:\Projects\Pipedrive\.git\config` ma remote `https://ghp_***ZREWOKOWAC***REDACTED***@github.com/eseen44/PipedriveScripts.git` — żywy token GitHub w czystym tekście na dysku. Do zrobienia: (a) zrewokować ten PAT na GitHub NATYCHMIAST, (b) `git remote set-url origin https://github.com/eseen44/PipedriveScripts.git` (czysty URL), (c) przy następnym pushu zalogować się przez Credential Manager (ten sam wzorzec co naprawiono dla BroadsignApi 2026-07-20).
3. **Tokeny Pipedrive w .pyc — wciąż w historii gita.** Tracking zatrzymany (`e632808`/`45fec58`), ale pliki `.pyc` z tokenami są w KAŻDYM commicie od `d7e2920` ("first commit") aż do cleanupu — wciąż w historii/na GitHub. Wymaga BFG (jak popstats) + rotacji samych tokenów Pipedrive (nie tylko czyszczenia historii).
4. MagicInfo `verify=False` (self-signed cert, serwer wewnętrzny) — zaakceptowany tradeoff, niższy priorytet, do rozważenia tylko jeśli będzie właściwy certyfikat dla `mi.stroeer.pl`.

## GitHub
Repo: `https://github.com/eseen44/BroadsignApi`  Branch: `main`.
