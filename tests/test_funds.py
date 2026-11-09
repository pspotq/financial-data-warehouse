"""Fund-filing parsers in cloud/feeds/sec_funds.py (Form D, Form 144, Schedule 13G, 13F) on real EDGAR full submissions.
Each expected value was checked by hand against the filing's own XML (b246, Oct 30 2026)."""
import datetime as dt
import json

from conftest import load, fixture

sf = load("cloud/feeds/sec_funds.py", "sec_funds")


def test_form_d_4pg_trailer_fund():
    # D 0002160312-26-000001: new LLC offering, $725,000 sold of $725,000 to 8 investors, Rule 506(b)
    r = sf.parse_formd("0002160312-26-000001", "D", "2026-10-28", fixture("formd_4pg_000216031226000001.txt"))
    assert r["entity_name"] == "4PG Trailer Fund LLC" and r["entity_type"] == "Limited Liability Company"
    assert (r["city"], r["state"]) == ("ORLANDO", "FL")
    assert r["industry_group"] == "Business Services"
    assert r["exemptions"] == "06b"
    assert r["is_amendment"] is False and r["prev_accession"] is None
    assert str(r["first_sale_date"]) == "2026-10-16"
    assert r["is_equity"] is True and r["is_debt"] is False and r["is_pooled_fund"] is False
    assert r["total_offering"] == 725000.0 and r["total_sold"] == 725000.0 and r["total_remaining"] == 0.0
    assert r["offering_indefinite"] is False
    assert r["investors_count"] == 8 and r["has_nonaccredited"] is False
    assert r["min_investment"] == 0.0 and r["sales_commissions"] == 0.0 and r["finders_fees"] == 0.0
    assert r["related_persons"] == "Jason Driskel (Director)"


def test_form_144_american_tower_officer():
    # 144 0001950047-26-010458: officer notice, 1,578 RSU shares (~$290K) under a 10b5-1 plan adopted Oct 29 2025
    rows = sf.parse_144("0001950047-26-010458", "144", "2026-10-28", fixture("form144_amt_000195004726010458.txt"))
    assert len(rows) == 1
    r = rows[0]
    assert r["issuer_cik"] == "1053507" and r["issuer_name"] == "AMERICAN TOWER CORPORATION"
    assert r["seller"] == "RUTH T DOWLING" and r["relationship"] == "Officer"
    assert r["units_to_sell"] == 1578.0 and r["market_value"] == 290083.74
    assert r["units_outstanding"] == 463763555.0
    assert str(r["approx_sale_date"]) == "2026-10-28" and r["exchange"] == "NYSE"
    assert str(r["acquired_date"]) == "2023-03-10" and r["acquisition_nature"] == "Restricted Stock Units"
    assert r["sold_past_3m_units"] == 1106.0 and r["sold_past_3m_value"] == 193505.76
    assert str(r["plan_adoption_date"]) == "2025-10-29"


def test_schedule_13g_franklin_albemarle():
    # SCHEDULE 13G 0000038777-26-000273: Franklin Templeton 5.0% of Albemarle (6,051,982 shares), two Johnsons at 0
    r = sf.parse_13g("0000038777-26-000273", "SCHEDULE 13G", "2026-10-28", fixture("form13g_alb_000003877726000273.txt"))
    assert r["issuer_name"] == "ALBEMARLE CORPORATION" and r["cusip"] == "012653101"
    assert str(r["event_date"]) == "2026-09-30"
    assert r["filer"] == "Franklin Templeton, Inc." and r["filer_type"] == "HC"
    assert r["pct"] == 5.0 and r["shares"] == 6051982.0
    assert (r["sole_vote"], r["shared_vote"]) == (5778234.0, 14543.0)
    assert (r["sole_dispositive"], r["shared_dispositive"]) == (6037376.0, 14606.0)
    assert r["rule"] == "Rule 13d-1(b)"
    persons = json.loads(r["persons"])
    assert [p["name"] for p in persons] == ["Franklin Templeton, Inc.", "Charles B. Johnson", "Rupert H. Johnson, Jr."]
    assert [p["shares"] for p in persons] == [6051982.0, 0.0, 0.0]


def test_13f_dollar_era_aaf_wealth():
    # 13F-HR 0001726609-26-000008 (Q3 2026): 65 lines, $275,147,318 in whole dollars (post-2023 rule)
    f, h = sf.parse_13f("0001726609-26-000008", "13F-HR", "2026-10-28", "1726609",
                        fixture("form13f_aaf_000172660926000008.txt"))
    assert f["manager"] == "AAF Wealth Management, LLC" and str(f["period"]) == "2026-09-30"
    assert f["entry_total"] == 65 and f["value_total_usd"] == 275147318.0 and f["other_managers_count"] == 4
    assert len(h) == 65
    amd = next(x for x in h if x["issuer"] == "ADVANCED MICRO DEVICES INC")
    assert amd["cusip"] == "007903107" and amd["value_usd"] == 298539.0 and amd["shares"] == 488.0
    assert amd["discretion"] == "DFND" and amd["other_manager"] == "2, 4" and amd["vote_none"] == 488.0
    totals, fixes = sf._units(f, h)
    assert totals["holdings_value_usd"] == 275147318.0 and totals["holdings_lines"] == 65
    assert fixes == []   # holdings already sum to the cover total in dollars


def test_13f_thousands_era_ams_capital():
    # 13F-HR 0001717977-22-000008 (Q3 2022): values reported in $ thousands (cover 158,118), stored as dollars
    f, h = sf.parse_13f("0001717977-22-000008", "13F-HR", "2022-10-27", "1717977",
                        fixture("form13f_ams_000171797722000008.txt"))
    assert f["manager"] == "AMS Capital Ltda" and f["entry_total"] == 12
    assert f["value_total_usd"] == 158118000.0
    cp = next(x for x in h if x["cusip"] == "13645T100")
    assert cp["value_reported"] == 4011.0 and cp["value_usd"] == 4011000.0 and cp["shares"] == 54814.0
    assert sf._units(f, h)[0]["holdings_value_usd"] == 158118000.0
