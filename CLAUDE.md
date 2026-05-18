# BroadsignApi — kontekst projektu

## Co to jest
Projekt do wyciągania danych z Broadsign Direct API i eksportu do CSV/Excel.
Firma: Stroer Polska. Uzytkownik: jratajski@stroeer.pl (Admin).

## Struktura projektu
```
BroadsignApi/
├── Package/
│   ├── auth.py            # logowanie, sesja z cookie
│   ├── proposals.py       # kampanie (proposals)
│   ├── proposal_items.py  # line itemy
│   ├── inventory.py       # ekrany + mapping screen<->frame
│   └── reporting.py       # fill rate breakdown
├── Scripts/
│   ├── get_inventory.py        # ekrany + mapping -> xlsx
│   ├── get_fill_rate.py        # fill rate ostatnie 7 dni -> xlsx
│   ├── get_all_proposals.py    # wszystkie kampanie -> xlsx
│   ├── get_all_proposal_items.py # wszystkie line itemy -> xlsx
│   └── export_joined.py        # all-in-one join -> csv
├── Data/                  # gitignored - tylko lokalne pliki
│   ├── proposals_x_lineitems.csv  # GLOWNY PLIK: 3628 rekordow, 68 kol
│   ├── joined_campaigns.csv       # fill rate x line items x proposals
│   ├── joined_screens.csv         # ekrany x frames mapping
│   ├── all_proposals.xlsx         # 1124 kampanii (pelne kolumny)
│   ├── all_proposal_items.xlsx    # 3628 line itemow (pelne kolumny)
│   ├── inventory_screens.xlsx     # 92 ekrany digital
│   ├── screens_frames_mapping.xlsx # 321 rekordow screen<->frame
│   ├── fill_rate_breakdown.xlsx   # fill rate ostatnie 7 dni
│   └── users.xlsx                 # 59 uzytkownikow (lookup id->email+rola)
├── .env                   # gitignored - credentials
├── cookies.json           # gitignored - sesja Direct API
└── CLAUDE.md              # ten plik
```

## Autentykacja
- **Direct API**: session cookie (`POST /login` -> `cookies.json`)
  - URL: `https://direct.broadsign.com/api/v1/`
  - Credentials w `.env`: `BROADSIGN_EMAIL`, `BROADSIGN_PASSWORD`
- **Control API key**: `9f8c0ef640cdd9d405dc4e2a9f492328` (w `.env` jako `BROADSIGN_CONTROL_API_KEY`)
  - Status: nieuzywany - nie znaleziono jeszcze Control API servera
  - Instancja wyglada na w pelni cloud, brak portu 10889

## Python
`C:\ProgramData\anaconda3\python.exe` — uzywac tej sciezki, nie `python`/`py`

## Jak uruchamiac skrypty
```
cd C:/Projects/BroadsignApi
"C:/ProgramData/anaconda3/python.exe" Scripts/export_joined.py
```

## Struktura API (Direct API)

### Kluczowe endpointy
| Endpoint | Metoda | Opis |
|----------|--------|------|
| `POST /proposal/search` | POST | kampanie, paginacja: `$skip/$top`, resp: `total`/`data` |
| `POST /proposal_item/search` | POST | line itemy, resp: `total_rows`/`proposal_items` |
| `POST /screen/search` | POST | ekrany, wymaga `inventory_type: "digital"` |
| `GET /reporting/screens_frames_mapping` | GET | mapping screen<->frame |
| `POST /reporting/fill_rate_breakdown` | POST | fill rate, resp: `total`/`data.proposal_items` |
| `POST /user/search` | POST | uzytkownicy, resp: `d.__count`/`d.results` |
| `GET /domain` | GET | konfiguracja domeny (priority levels, ustawienia) |
| `GET /user/current_user` | GET | sprawdzenie czy sesja zywa |

### Jak dziala paginacja
```python
body = {"$skip": 0, "$top": 100}
# proposals ma tez sort: "$sort": [{"field": "id", "dir": "desc"}]
```

## Schemat joinow
```
proposals (campaign_id=id)
    └── proposal_items (campaign_id -> proposals.id)
            └── fill_rate (line_id -> proposal_items.id)

screens (id)
    └── screens_frames_mapping (group_id -> screens.id)
            zawiera: screen_id, frame_id, screen_uuid, group_uuid

users (id -> proposal_items.owner_user_id)
```

## Co zostalo do zrobienia
- [ ] Dolaczenie ekranow do proposals_x_lineitems
      (brakuje bezposredniego klucza line_item -> screen_id;
       mozliwe przez `POST /screen/search` z `proposal_item_id` jako filtr)
- [ ] Control API — znalezc hostname serwera
      (nie jest w CSP app.broadsign.com, prawdopodobnie on-premise)
- [ ] Historyczne logi emisji (wymaga Control API lub innego endpointu)
- [ ] Dane finansowe / revenue (endpoint `/domain` ma `export_csv_revenue_date`)

## GitHub
Repo: `https://github.com/eseen44/BroadsignApi`
Branch: `main`
Token w remote URL (skonfigurowany)
