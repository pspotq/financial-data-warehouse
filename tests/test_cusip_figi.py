"""OpenFIGI lookup (b190): picks the US listing and records not-found CUSIPs, with no network."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "cloud", "feeds"))
import cusip_figi as C  # noqa: E402


def test_pick_prefers_us_composite():
    data = [{"exchCode": "LN", "ticker": "X1"}, {"exchCode": "UN", "ticker": "ABC"}, {"exchCode": "US", "ticker": "ABC"}]
    assert C.pick(data)["exchCode"] == "US"
    assert C.pick([{"exchCode": "LN", "ticker": "X1"}])["ticker"] == "X1"


def test_lookup_rows(monkeypatch):
    monkeypatch.setattr(C.time, "sleep", lambda s: None)
    calls = []

    def post(url, body, headers):
        calls.append(body)
        return 200, [{"data": [{"exchCode": "US", "ticker": "BRK/B", "name": "BERKSHIRE HATHAWAY INC-CL B",
                                "securityType": "Common Stock", "marketSector": "Equity", "figi": "F1"}]},
                     {"warning": "No identifier found."}]

    rows = C.lookup([("08467070", "084670702"), ("99999999", "999999999")], post)
    assert len(calls) == 1 and calls[0][0] == {"idType": "ID_CUSIP", "idValue": "084670702"}
    assert rows[0]["found"] and rows[0]["ticker"] == "BRK/B" and rows[0]["market_sector"] == "Equity"
    assert not rows[1]["found"] and rows[1]["note"] == "No identifier found."


def test_lookup_stops_on_error(monkeypatch):
    monkeypatch.setattr(C.time, "sleep", lambda s: None)
    rows = C.lookup([("1", "1")] * 25, lambda u, b, h: (500, None), log=lambda *_: None)
    assert rows == []
