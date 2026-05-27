BASE_URL = "https://direct.broadsign.com/api/v1"
PAGE_SIZE = 100


def get_proposal_item(session, item_id):
    resp = session.get(f"{BASE_URL}/proposal/proposal_item/{item_id}")
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    return resp.json()


def get_proposal_item_summary(session, item_id):
    resp = session.get(f"{BASE_URL}/proposal/proposal_item/summary/{item_id}")
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    return resp.json()


def search_proposal_items(session, skip=0, top=PAGE_SIZE, filters=None):
    body = {"$skip": skip, "$top": top}
    if filters:
        body.update(filters)
    resp = session.post(f"{BASE_URL}/proposal_item/search", json=body)
    if not resp.ok:
        raise Exception(f"Błąd API: {resp.status_code} {resp.text}")
    return resp.json()


def get_all_proposal_items(session, filters=None):
    first = search_proposal_items(session, skip=0, top=PAGE_SIZE, filters=filters)
    total = first["total_rows"]
    results = first["proposal_items"]

    print(f"Łącznie: {total} line itemów. Pobieranie...", flush=True)

    skip = PAGE_SIZE
    while skip < total:
        page = search_proposal_items(session, skip=skip, top=PAGE_SIZE, filters=filters)
        results.extend(page["proposal_items"])
        print(f"  {min(skip + PAGE_SIZE, total)}/{total}", flush=True)
        skip += PAGE_SIZE

    return results


def get_items_for_proposal(session, proposal):
    item_ids = proposal.get("proposal_item_id_list", [])
    items = []
    for item_id in item_ids:
        item = get_proposal_item(session, item_id)
        summary = get_proposal_item_summary(session, item_id)
        item["screen_count"] = summary.get("screen_count")
        item["audience_count"] = summary.get("audience_count")
        items.append(item)
    return items
