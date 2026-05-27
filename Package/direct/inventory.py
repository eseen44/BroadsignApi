BASE_URL = "https://direct.broadsign.com/api/v1"
PAGE_SIZE = 100


def search_screens(session, inventory_type="digital", skip=0, top=PAGE_SIZE, filters=None):
    body = {"inventory_type": inventory_type, "$skip": skip, "$top": top}
    if filters:
        body.update(filters)
    resp = session.post(f"{BASE_URL}/screen/search", json=body)
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    return resp.json()


def get_all_screens(session, inventory_type="digital"):
    first = search_screens(session, inventory_type=inventory_type)
    total = first["total"]
    results = first["data"]

    print(f"Inventory [{inventory_type}]: {total} ekranów", flush=True)

    skip = PAGE_SIZE
    while skip < total:
        page = search_screens(session, inventory_type=inventory_type, skip=skip)
        results.extend(page["data"])
        skip += PAGE_SIZE

    return results


def get_screens_frames_mapping(session):
    resp = session.get(f"{BASE_URL}/reporting/screens_frames_mapping")
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    return resp.json()
