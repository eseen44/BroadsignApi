"""
MagicInfo explore -- jednorazowy skrypt do badania API.
Uruchamiaj z roota projektu: python -m Package.magicinfo.explore

Drukuje raw JSON z kluczowych endpointow zeby zobaczyc co jest dostepne
i jak wyglada struktura danych przed pisaniem pipeline'u.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass

from Package.magicinfo.client import MagicInfoClient


def pp(label: str, data) -> None:
    raw = json.dumps(data, indent=2, ensure_ascii=False, default=str)
    print(f"\n{'='*60}")
    print(f"  {label}")
    print("="*60)
    print(raw[:3000])
    if len(raw) > 3000:
        print("  ... [obcieto do 3000 znakow]")


def explore_device_groups(mi: MagicInfoClient) -> None:
    print("\n>>> DEVICE GROUPS")
    for path in ("restapi/v1.0/rms/devices/groups", "restapi/v2.0/rms/devices/groups"):
        try:
            pp(f"GET /{path}", mi.get(path))
        except Exception as e:
            print(f"  [{_status(e)}] {path} -- {e}")


def explore_devices(mi: MagicInfoClient) -> None:
    print("\n>>> DEVICES (pierwsze 5)")
    for path in ("restapi/v1.0/rms/devices", "restapi/v2.0/rms/devices"):
        try:
            pp(f"GET /{path}", mi.get(path, params={"pageSize": 5, "startIndex": 1}))
        except Exception as e:
            print(f"  [{_status(e)}] {path} -- {e}")

    print("\n>>> DEVICES filter -- szukamy liveline / stroer / metro")
    for keyword in ["liveline", "stroer", "metro", "wagon"]:
        try:
            data = mi.post(
                "restapi/v1.0/rms/devices/filter",
                body={"deviceName": keyword, "pageSize": 5, "startIndex": 1},
            )
            pp(f"filter deviceName={keyword!r}", data)
        except Exception as e:
            print(f"  [{_status(e)}] filter={keyword!r} -- {e}")


def explore_statistics(mi: MagicInfoClient) -> None:
    PATH = "restapi/v2.0/ems/statistics/contents"
    print(f"\n>>> STATISTICS -- POST /{PATH}")

    today = "2026-07-01"

    # [1] playFrequency -- yesterday (nie wymaga contentIds/groupIds wg Swagger?)
    print("\n  [1] playFrequency / yesterday / bez filtrow")
    try:
        pp("playFrequency yesterday", mi.post(PATH, body={
            "data":   "playFrequency",
            "format": "frequencyTable",
            "time":   "yesterday",
            "unit":   "day",
        }))
    except Exception as e:
        _http_error("playFrequency yesterday", e)

    # [2] playFrequency -- yesterday / groupId=1 (STROER calosc)
    print("\n  [2] playFrequency / yesterday / groupId=1")
    try:
        pp("playFrequency groupId=1", mi.post(PATH, body={
            "data":     "playFrequency",
            "format":   "frequencyTable",
            "groupIds": [1],
            "time":     "yesterday",
            "unit":     "day",
        }))
    except Exception as e:
        _http_error("playFrequency groupId=1", e)

    # [3] playFrequency -- custom date range
    print("\n  [3] playFrequency / custom startDate=endDate=today")
    try:
        pp("playFrequency custom today", mi.post(PATH, body={
            "data":      "playFrequency",
            "format":    "frequencyTable",
            "groupIds":  [1],
            "time":      "custom",
            "unit":      "day",
            "startDate": today,
            "endDate":   today,
        }))
    except Exception as e:
        _http_error("playFrequency custom today", e)

    # [4] popFileHistoryPeriods -- jakie okresy sa dostepne?
    print("\n  [4] popFileHistoryPeriods")
    try:
        pp("popFileHistoryPeriods", mi.post(PATH, body={
            "data": "popFileHistoryPeriods",
        }))
    except Exception as e:
        _http_error("popFileHistoryPeriods", e)

    # [5] popFileHistory / receivedChart -- ktore urzadzenia przeslaly PoP
    print("\n  [5] popFileHistory / receivedChart")
    try:
        pp("popFileHistory receivedChart", mi.post(PATH, body={
            "data":      "popFileHistory",
            "format":    "receivedChart",
            "startDate": today,
            "pageSize":  10,
            "startIndex": 1,
        }))
    except Exception as e:
        _http_error("popFileHistory receivedChart", e)

    # [6] popFileHistory / notReceivedChart
    print("\n  [6] popFileHistory / notReceivedChart")
    try:
        pp("popFileHistory notReceivedChart", mi.post(PATH, body={
            "data":      "popFileHistory",
            "format":    "notReceivedChart",
            "startDate": today,
            "pageSize":  10,
            "startIndex": 1,
        }))
    except Exception as e:
        _http_error("popFileHistory notReceivedChart", e)

    # [7] countByDate -- ogolna statystyka
    print("\n  [7] countByDate / typeTable")
    try:
        pp("countByDate typeTable", mi.post(PATH, body={
            "data":      "countByDate",
            "format":    "typeTable",
            "startDate": "2026-06-01",
            "endDate":   today,
        }))
    except Exception as e:
        _http_error("countByDate typeTable", e)


def explore_analysis(mi: MagicInfoClient) -> None:
    print("\n>>> ANALYSIS / INSIGHT -- discovery")

    # top10s: najpierw bez params (Swagger nie pokazuje pageSize), potem z
    for label, params in [("bez params", None), ("z pageSize", {"pageSize": 5, "startIndex": 1})]:
        path = "restapi/v2.0/analysis/insight/top10s"
        try:
            pp(f"GET /{path} [{label}]", mi.get(path, params=params))
        except Exception as e:
            _http_error(f"GET /{path} [{label}]", e)

    # pozostale kandydaty -- bez params
    candidates = [
        "restapi/v2.0/analysis/insight",
        "restapi/v2.0/analysis",
        "restapi/v2.0/analysis/insight/play",
        "restapi/v2.0/analysis/insight/playLog",
        "restapi/v2.0/analysis/insight/playHistory",
        "restapi/v2.0/analysis/insight/proofOfPlay",
        "restapi/v2.0/analysis/insight/pop",
        "restapi/v2.0/analysis/insight/statistics",
        "restapi/v2.0/analysis/insight/report",
        "restapi/v2.0/analysis/insight/contents",
        "restapi/v2.0/analysis/insight/devices",
        "restapi/v2.0/analysis/insight/summary",
        "restapi/v2.0/analysis/insight/daily",
        "restapi/v2.0/analysis/insight/hourly",
        "restapi/v2.0/ems/statistics",
        "restapi/v2.0/cms/statistics",
        "restapi/v2.0/dms/statistics",
        "restapi/v2.0/rms/statistics",
        "restapi/v2.0/statistics/insight",
    ]

    for path in candidates:
        try:
            pp(f"GET /{path}", mi.get(path))
        except Exception as e:
            _http_error(f"GET /{path}", e)


def explore_schedules(mi: MagicInfoClient) -> None:
    print("\n>>> SCHEDULES (pierwsze 3)")
    try:
        pp("GET /dms/schedule/contents", mi.get(
            "restapi/v1.0/dms/schedule/contents", params={"pageSize": 3, "startIndex": 1}
        ))
    except Exception as e:
        print(f"  [{_status(e)}] schedules -- {e}")


def _status(e: Exception) -> str:
    return str(getattr(getattr(e, "response", None), "status_code", "?"))


def _http_error(prefix: str, e: Exception) -> None:
    resp = getattr(e, "response", None)
    status = getattr(resp, "status_code", "?")
    print(f"  [{status}] {prefix}")
    if resp is not None:
        print(f"  URL:  {resp.url}")
        print(f"  BODY: {(resp.text or '')[:2000]}")


if __name__ == "__main__":
    print("=== MagicInfo API Explorer ===")
    print("Wyniki obciete do 3000 znakow kazdego endpointu\n")

    with MagicInfoClient() as mi:
        explore_device_groups(mi)
        explore_devices(mi)
        explore_schedules(mi)
        explore_analysis(mi)
        explore_statistics(mi)

    print("\n=== Koniec eksploracji ===")
    input("\nNacisnij Enter zeby zamknac...")