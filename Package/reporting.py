from datetime import date, timedelta

BASE_URL = "https://direct.broadsign.com/api/v1"
PAGE_SIZE = 100


def get_fill_rate_breakdown(session, screen_ids, start_date=None, end_date=None,
                             start_time="00:00:00", end_time="23:59:59",
                             skip=0, top=PAGE_SIZE):
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date is None:
        start_date = end_date - timedelta(days=6)

    body = {
        "screen_ids": screen_ids,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "start_time": start_time,
        "end_time": end_time,
        "$skip": skip,
        "$top": top,
    }
    resp = session.post(f"{BASE_URL}/reporting/fill_rate_breakdown", json=body)
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    return resp.json()


def get_all_fill_rate(session, screen_ids, start_date=None, end_date=None):
    first = get_fill_rate_breakdown(session, screen_ids, start_date, end_date,
                                    skip=0, top=PAGE_SIZE)
    total = first["total"]
    results = first["data"]["proposal_items"]

    print(f"Fill rate: {total} rekordow. Pobieranie...", flush=True)

    skip = PAGE_SIZE
    while skip < total:
        page = get_fill_rate_breakdown(session, screen_ids, start_date, end_date,
                                       skip=skip, top=PAGE_SIZE)
        results.extend(page["data"]["proposal_items"])
        print(f"  {min(skip + PAGE_SIZE, total)}/{total}", flush=True)
        skip += PAGE_SIZE

    return results
