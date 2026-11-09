"""The subject split (rebuild b252, 2026-10-29): which market sources each job runs at a given time. rrp-markets must never
touch the agency sources, rrp-gov must run only them, and gov's daily agency pass must land after the 22:20 UTC slot the
health board checks. Offline: every puller is replaced by a stub that records its name."""
import datetime as real_dt
import sys
import types

from conftest import load


def ran(subject, when, argv=()):
    import os
    os.environ["RRP_SUBJECT"] = subject
    old = sys.argv
    sys.argv = ["main.py", "--dry", *argv]   # module-level flags (--equities-only) are read at import
    try:
        m = load("cloud/market/main.py", f"market_{subject}_{when:%H%M}")
    finally:
        os.environ.pop("RRP_SUBJECT", None)
    seen = []
    m.PULLERS = [(n, (lambda n: lambda now: seen.append(n) or {})(n)) for n, _ in m.PULLERS]

    class FakeDT(real_dt.datetime):
        @classmethod
        def now(cls, tz=None): return when.replace(tzinfo=real_dt.timezone.utc)
    m.dt = types.SimpleNamespace(datetime=FakeDT, timezone=real_dt.timezone, timedelta=real_dt.timedelta, date=real_dt.date)
    try:
        m.run(dry=True)
    finally:
        sys.argv = old
    return set(seen), m


AGENCY = {"macro_treasury", "treasury_dts", "eia", "eia_weekly", "degree_days"}


def test_markets_never_runs_agency_sources():
    top, m = ran("markets", real_dt.datetime(2026, 10, 29, 14, 0))
    assert not top & AGENCY
    assert {"coinbase", "kraken_ticker", "macro_fred", "macro_yahoo", "farside_etf"} <= top
    assert "macro_cboe" not in {n for n, _ in m.PULLERS}
    eq, _ = ran("markets", real_dt.datetime(2026, 10, 29, 22, 20), ["--equities-only"])
    assert {"equities", "universe", "short_volume", "etf_shares"} <= eq and not eq & AGENCY


def test_gov_hourly_and_evening_pass():
    hourly, _ = ran("gov", real_dt.datetime(2026, 10, 29, 14, 25))
    assert hourly == {"macro_treasury"}
    evening, _ = ran("gov", real_dt.datetime(2026, 10, 29, 22, 25))   # Thursday
    assert evening == AGENCY
    weekend, _ = ran("gov", real_dt.datetime(2026, 10, 31, 22, 25))   # Saturday
    assert weekend == {"macro_treasury"}
