"""
Broadsign Control API client.
Auth: Bearer token (BROADSIGN_CONTROL_API_KEY z .env)
Base URL: https://api.broadsign.com:10889/rest
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.broadsign.com:10889/rest"
DOMAIN_ID = 925762247  # Ströer Polska — z /domain/v5


def get_session() -> requests.Session:
    api_key = os.getenv("BROADSIGN_CONTROL_API_KEY")
    if not api_key:
        raise EnvironmentError("Brak BROADSIGN_CONTROL_API_KEY w .env")
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_key}",
        "accept": "application/json",
    })
    return session


def fetch_resource(
    session: requests.Session,
    resource: str,
    version: int,
    since: str = "1970-01-01T00:00:00.",
    extra_params: dict = None,
    timeout: int = 60,
) -> tuple[list, str]:
    """
    Pobiera wszystkie rekordy danego zasobu Control API.

    Returns:
        (records: list[dict], not_modified_since: str)
        not_modified_since można zapisać i użyć jako 'since' przy następnym fetchu
        żeby pobrać tylko zmiany (incremental).
    """
    url = f"{BASE_URL}/{resource}/v{version}"
    params = {"not_modified_since": since}
    if extra_params:
        params.update(extra_params)

    resp = session.get(url, params=params, timeout=timeout)
    resp.raise_for_status()

    data = resp.json()
    records = data.get(resource, [])
    new_since = data.get("not_modified_since", "")

    print(f"  [{resource}/v{version}] {len(records)} rekordów  (cursor: {new_since})")
    return records, new_since
