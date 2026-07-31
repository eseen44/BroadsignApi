"""
Wspolna obsluga S3 dla pipeline'u (sync gold -> Athena, backup bronze).

Dlaczego osobny modul: oba miejsca potrzebuja tego samego -- klienta z sensownymi
timeoutami, ponawiania przy bledach przejsciowych i rozroznienia bledu, ktory
ma sens ponawiac, od takiego ktory go nie ma.
"""
import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    ConnectionError as BotoConnectionError,
    EndpointConnectionError,
)

# Kody bledow, ktorych NIE ma sensu ponawiac -- to blad konfiguracji/uprawnien,
# nie chwilowa awaria. Ponawianie ich tylko opoznia zglos<enie problemu.
BLEDY_TRWALE = {
    "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
    "NoSuchBucket", "AllAccessDisabled", "InvalidBucketName",
    "ExpiredToken", "InvalidToken", "AuthorizationHeaderMalformed",
}

PROBY = 4
BACKOFF_S = 3        # 3s, 6s, 12s -- rosnaco


def make_client():
    """Klient S3 z wlasnym retry botocore + rozsadnymi timeoutami.

    Ustawiamy timeouty jawnie, bo domyslne (60s connect) potrafia zawiesic
    crona na dlugo, gdy VPN padnie w trakcie -- a to sie tu zdarza regularnie.
    """
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        config=Config(
            retries={"max_attempts": 3, "mode": "standard"},   # warstwa botocore
            connect_timeout=15,
            read_timeout=120,
        ),
    )


def _trwaly(exc) -> bool:
    return (isinstance(exc, ClientError)
            and exc.response["Error"]["Code"] in BLEDY_TRWALE)


def _opis(exc) -> str:
    if isinstance(exc, ClientError):
        e = exc.response["Error"]
        return f"{e['Code']}: {e['Message'][:120]}"
    return f"{type(exc).__name__}: {str(exc)[:120]}"


def upload_retry(s3, sciezka, bucket: str, key: str, etykieta: str = "") -> None:
    """Wysyla plik, ponawiajac przy bledach przejsciowych. Rzuca po wyczerpaniu prob.

    Warstwa botocore ma wlasne retry (3x, na poziomie pojedynczego zadania HTTP),
    ta petla jest NAD nia -- lapie przypadki, w ktorych padl caly transfer, np.
    zerwany VPN w polowie 60 MB pliku.
    """
    ostatni = None
    for proba in range(1, PROBY + 1):
        try:
            s3.upload_file(str(sciezka), bucket, key)
            return
        except (ClientError, EndpointConnectionError, BotoConnectionError, BotoCoreError) as e:
            ostatni = e
            if _trwaly(e):
                raise                       # AccessDenied itp. -- nie ma czego ponawiac
            if proba < PROBY:
                czekaj = BACKOFF_S * (2 ** (proba - 1))
                print(f"      proba {proba}/{PROBY} nieudana ({_opis(e)}) "
                      f"-- ponawiam za {czekaj}s")
                time.sleep(czekaj)
    raise ostatni


def rozmiar_na_s3(s3, bucket: str, key: str) -> int | None:
    """Rozmiar obiektu wg listingu, albo None gdy go nie ma.

    Uzywamy list_objects_v2 zamiast head_object celowo -- konto powerbi.prod ma
    ListBucket, ale (stan 2026-07-31) NIE ma GetObject, a head_object wymaga
    tego drugiego i zwrocilby 403 mimo ze plik istnieje.
    """
    r = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    for o in r.get("Contents", []):
        if o["Key"] == key:
            return o["Size"]
    return None
