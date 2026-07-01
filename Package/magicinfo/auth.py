"""
MagicInfo auth -- TOTP + token management.

Token cachowany w magicinfo_token.json przy root projektu.
Przy kazdym get_token() sprawdzamy czy zapisany token jeszcze zyje
(zakladamy 1h TTL jesli API nie zwraca expiry). Przy 401 odswieza.
"""
import json
import os
import time
from pathlib import Path

import pyotp
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL       = os.getenv("MI_BASE_URL", "https://mi.stroeer.pl:7002/MagicInfo")
MI_USER        = os.getenv("MI_USER")
MI_PASSWORD    = os.getenv("MI_PASSWORD")
MI_TOTP_SECRET = os.getenv("MI_TOTP_SECRET")

for _var, _name in [(MI_USER, "MI_USER"), (MI_PASSWORD, "MI_PASSWORD"), (MI_TOTP_SECRET, "MI_TOTP_SECRET")]:
    if not _var:
        raise EnvironmentError(f"Brak {_name} w .env")

TOKEN_FILE = Path(__file__).resolve().parent.parent.parent / "magicinfo_token.json"
TOKEN_TTL  = 1800  # sekund -- 30 min (z JWT: expired-created = 1800s)


def _generate_otp() -> str:
    return pyotp.TOTP(MI_TOTP_SECRET).now()


def _do_auth() -> dict:
    otp = _generate_otp()
    resp = requests.post(
        f"{BASE_URL}/restapi/v2.0/auth",
        json={"grantType": "password", "username": MI_USER, "password": MI_PASSWORD, "totp": otp},
        verify=False,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _save_token(data: dict) -> None:
    payload = {
        "token":        data.get("token") or data.get("accessToken") or (data.get("data") or {}).get("token"),
        "refreshToken": data.get("refreshToken") or (data.get("data") or {}).get("refreshToken"),
        "fetched_at":   time.time(),
    }
    TOKEN_FILE.write_text(json.dumps(payload, indent=2))


def _load_token() -> dict | None:
    if not TOKEN_FILE.exists():
        return None
    try:
        payload = json.loads(TOKEN_FILE.read_text())
        age = time.time() - payload.get("fetched_at", 0)
        if age < TOKEN_TTL - 60:
            return payload
    except Exception:
        pass
    return None


def get_token(force_refresh: bool = False) -> str:
    """Zwraca wazny token API. Uzywa cache lub loguje przez TOTP."""
    if not force_refresh:
        cached = _load_token()
        if cached and cached.get("token"):
            return cached["token"]

    print("  [magicinfo] Logowanie przez TOTP...")
    data = _do_auth()
    _save_token(data)

    token = (
        data.get("token")
        or data.get("accessToken")
        or (data.get("data") or {}).get("token")
    )
    if not token:
        raise RuntimeError(f"Brak tokena w odpowiedzi /auth: {data}")

    print("  [magicinfo] Token pobrany.")
    return token
