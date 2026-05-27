SEARCH_URL = "https://direct.broadsign.com/api/v1/proposal/search"
PAGE_SIZE = 100


def search_proposals(session, skip=0, top=PAGE_SIZE, sort_field="id", sort_dir="desc", filters=None):
    body = {
        "$skip": skip,
        "$top": top,
        "$sort": [{"field": sort_field, "dir": sort_dir}],
    }
    if filters:
        body.update(filters)
    resp = session.post(SEARCH_URL, json=body)
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    return resp.json()


def get_all_proposals(session, sort_field="id", sort_dir="desc"):
    first = search_proposals(session, skip=0, top=PAGE_SIZE, sort_field=sort_field, sort_dir=sort_dir)
    total = first["total"]
    results = first["data"]

    print(f"Łącznie: {total} kampanii. Pobieranie...", flush=True)

    skip = PAGE_SIZE
    while skip < total:
        page = search_proposals(session, skip=skip, top=PAGE_SIZE, sort_field=sort_field, sort_dir=sort_dir)
        results.extend(page["data"])
        print(f"  {min(skip + PAGE_SIZE, total)}/{total}", flush=True)
        skip += PAGE_SIZE

    return results
