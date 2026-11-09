"""feeds main.pull_edgar_index: counts per form from a daily form index (b191)."""
import datetime as dt
from conftest import load

IDX = """Form Type   Company Name                                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
4                ACME CORP                                                     1234567     20261023    edgar/data/1234567/0001234567-26-000001.txt
4                SMITH JOHN                                                    7654321     20261023    edgar/data/7654321/0001234567-26-000001.txt
4/A              BETA INC                                                      2222222     20261023    edgar/data/2222222/0002222222-26-000009.txt
SCHEDULE 13G     GAMMA LTD                                                     3333333     20261023    edgar/data/3333333/0003333333-26-000004.txt
D                FUND LP                                                       4444444     20261023    edgar/data/4444444/0004444444-26-000002.txt
"""


class R:
    text = IDX


def test_counts_distinct_filings_per_form(monkeypatch):
    m = load("cloud/feeds/main.py", "feeds_main")
    monkeypatch.setattr(m, "get", lambda url, sec=False, **k: R() if "20261023" in url else None)
    out, note = m.pull_edgar_index(dt.datetime(2026, 10, 27, 8, 5, tzinfo=dt.timezone.utc), days=5)
    got = {r["form"]: (r["filings"], r["lines"]) for r in out["edgar_index"]}
    assert got == {"4": (1, 2), "4/A": (1, 1), "SCHEDULE 13G": (1, 1), "D": (1, 1)}   # a joint Form 4 is one filing
    assert all(r["day"] == "2026-10-23" for r in out["edgar_index"])
    assert note.startswith("1 daily indexes")
