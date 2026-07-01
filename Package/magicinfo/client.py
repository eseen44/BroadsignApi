"""
MagicInfo API client.

Uzycie:
    from Package.magicinfo.client import MagicInfoClient
    with MagicInfoClient() as mi:
        devices = mi.get("restapi/v2.0/rms/devices", params={"pageSize": 100, "startIndex": 1})
"""
import requests
import urllib3
from Package.magicinfo.auth import BASE_URL, get_token

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MagicInfoClient:
    def __init__(self):
        self._session = requests.Session()
        self._token   = None

    def __enter__(self):
        self._token = get_token()
        return self

    def __exit__(self, *_):
        self._session.close()

    def _headers(self) -> dict:
        return {"api_key": self._token, "Content-Type": "application/json"}

    def _url(self, path: str) -> str:
        path = path.lstrip("/")
        return f"{BASE_URL}/{path}"

    def get(self, path: str, params: dict = None) -> dict:
        resp = self._session.get(
            self._url(path), headers=self._headers(), params=params, verify=False, timeout=30,
        )
        if resp.status_code == 401:
            self._token = get_token(force_refresh=True)
            resp = self._session.get(
                self._url(path), headers=self._headers(), params=params, verify=False, timeout=30,
            )
        resp.raise_for_status()
        return resp.json()

    def post(self, path: str, body: dict = None, params: dict = None, timeout: int = 30) -> dict:
        resp = self._session.post(
            self._url(path), headers=self._headers(), json=body, params=params, verify=False, timeout=timeout,
        )
        if resp.status_code == 401:
            self._token = get_token(force_refresh=True)
            resp = self._session.post(
                self._url(path), headers=self._headers(), json=body, params=params, verify=False, timeout=timeout,
            )
        resp.raise_for_status()
        return resp.json()

    def get_paged(self, path: str, params: dict = None, page_size: int = 100) -> list[dict]:
        """Iteruje przez wszystkie strony endpointu z pageSize/startIndex."""
        params = dict(params or {})
        params["pageSize"]   = page_size
        params["startIndex"] = 1
        all_items = []
        while True:
            data  = self.get(path, params=params)
            items = _extract_items(data)
            if not items:
                break
            all_items.extend(items)
            if len(items) < page_size:
                break
            params["startIndex"] += page_size
        return all_items


def _extract_items(data: dict) -> list:
    """Probuje wyciagnac liste z roznych struktur odpowiedzi MagicInfo."""
    if isinstance(data, list):
        return data
    for key in ("items", "list", "contents", "deviceListBaseBean", "data"):
        val = data.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            for subkey in ("items", "list", "contents"):
                sub = val.get(subkey)
                if isinstance(sub, list):
                    return sub
    return []
