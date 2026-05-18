import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from datetime import date, timedelta
from Package.auth import get_session
from Package.inventory import get_all_screens
from Package.reporting import get_all_fill_rate

OUTPUT = Path(__file__).resolve().parent.parent / "Data" / "fill_rate_breakdown.xlsx"

DAYS_BACK = 7


if __name__ == "__main__":
    session = get_session()

    end_date   = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=DAYS_BACK - 1)
    print(f"Zakres: {start_date} -> {end_date}")

    # Pobierz screen_ids z inventory
    screens = get_all_screens(session, inventory_type="digital")
    screen_ids = [s["id"] for s in screens]
    print(f"Ekranów: {len(screen_ids)}")

    data = get_all_fill_rate(session, screen_ids, start_date, end_date)

    if not data:
        print("Brak danych fill rate dla tego zakresu.")
    else:
        df = pd.DataFrame(data)
        df.to_excel(OUTPUT, index=False)
        print(f"\nZapisano {len(df)} rekordów -> {OUTPUT}")
        print(df.dtypes)
        print(df.head())
