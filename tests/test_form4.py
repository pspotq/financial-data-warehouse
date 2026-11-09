"""form4_scan.parse on real filings, answers checked by hand against the XML (b246)."""
from conftest import load, fixture

f4 = load("cloud/feeds/form4_scan.py", "form4_scan")


def test_edry_repeated_purchase_lines_are_kept():
    # EuroDry director, Oct 16 2026: the filing itself lists the same 300-share purchase twice (footnotes F1 / F2)
    rows = f4.parse(fixture("form4_edry_000212748426000021.xml"), "2026-10-16")
    assert len(rows) == 2
    for r in rows:
        assert (r["ticker"], r["code"], r["kind"]) == ("EDRY", "P", "buy")
        assert r["shares"] == 300 and r["price"] == 48.1682 and r["value"] == 14450.46
        assert r["owned_after"] == 77088 and r["direct_indirect"] == "I"
        assert r["role"] == "Director" and r["plan_10b5_1"] is False and r["tx_date"] == "2026-10-15"
        assert r["issuer_cik"] == "1731388"


def test_agio_plan_sales_only_open_market_lines():
    # Agios CMO, Oct 23 2026: option exercises (M) and the derivative table are skipped; two 10b5-1 sales kept
    rows = f4.parse(fixture("form4_agio_000143922226000122.xml"), "2026-10-23")
    assert [(r["code"], r["shares"], r["price"], r["value"]) for r in rows] == [
        ("S", 1489, 37.53, -55882.17), ("S", 500, 37.52, -18760.0)]
    assert all(r["kind"] == "sale" and r["plan_10b5_1"] and r["role"] == "Chief Medical Officer" for r in rows)
    assert [r["owned_after"] for r in rows] == [54645, 54645]


def test_not_a_form4_returns_nothing():
    assert f4.parse("<html>no ownership document</html>", "2026-10-23") == []
