import json
import os
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

LOGIN_URL = "https://direct.broadsign.com/login"
COOKIES_FILE = Path(__file__).resolve().parent.parent / "cookies.json"
EMAIL = os.getenv("BROADSIGN_EMAIL")
PASSWORD = os.getenv("BROADSIGN_PASSWORD")


def save_cookies(session):
    with open(COOKIES_FILE, "w") as f:
        json.dump(session.cookies.get_dict(), f)


def load_cookies(session):
    with open(COOKIES_FILE, "r") as f:
        session.cookies.update(json.load(f))


def login():
    session = requests.Session()
    resp = session.post(LOGIN_URL, data={"email": EMAIL, "password": PASSWORD})
    if resp.status_code == 200 and "Logged In" in resp.text:
        print("Zalogowano pomyślnie")
        save_cookies(session)
        return session
    raise Exception(f"Błąd logowania: {resp.status_code} {resp.text}")


def get_session():
    session = requests.Session()
    if COOKIES_FILE.exists():
        load_cookies(session)
        resp = session.get("https://direct.broadsign.com/api/v1/user/current_user")
        if resp.status_code == 200:
            print("Używam zapisanych cookies")
            return session
        print("Cookie wygasło, loguję od nowa...")
    return login()
