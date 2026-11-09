"""Terms extractors (tender, 13D stake, redemption) on real filing text from sec.filing_docs. Expected values are the
rows the extractors wrote to BigQuery and were checked by hand against the filings."""
import datetime as dt

from conftest import load, fixture

tender = load("cloud/feeds/tender_terms.py", "tender_terms")
stake = load("cloud/feeds/stake_terms.py", "stake_terms")
redemption = load("cloud/feeds/redemption_terms.py", "redemption_terms")


def test_redemption_centene_full_call():
    # 8-K 0001071739-26-000186: all remaining $568,664,000 of 4.25% notes, redeemed Oct 23, 2026 at 100%
    r = redemption.extract("8-K", [(0, "INDEX", ""), (1, "8-K", fixture("redemption_cnc_000107173926000186.txt"))],
                           file_date=dt.date(2026, 10, 8))
    assert r["is_call"] is True and r["full_call"] is True and r["make_whole"] is False
    assert r["amount_usd"] == 568664000.0
    assert r["redemption_date"] == dt.date(2026, 10, 23)
    assert r["price_pct"] == 100.0
    assert r["security_kind"] is None and r["price_per_unit"] is None


def test_tender_medtronic_final_exchange_offer():
    # SC TO-I/A 0001628280-26-065823: Amendment No. 4, final results of the MiniMed split-off exchange offer
    r = tender.extract("SC TO-I/A", [(1, "SC TO-I/A", fixture("tender_mdt_000162828026065823.txt"))])
    assert r["subject_company"] == "MEDTRONIC PLC"
    assert r["filer"] == "MEDTRONIC PLC"        # v2 stored "and Filing Person" for this cover layout
    assert r["cusip"] == "G5960L103"
    assert r["offer_kind"] == "exchange_offer"
    assert r["is_final"] is True
    assert r["shares_tendered"] == 297521390.0
    assert r["expiration_date"] == dt.date(2026, 10, 9)
    assert r["price_per_share"] is None


def test_tender_cover_name_with_parenthetical():
    # interval-fund covers name the fund with its own parenthetical; v2 kept only ", LLC"
    t = ("SCHEDULE TO Tender Offer Statement Under Section 14(d)(1) or 13(e)(1) of the Securities Exchange Act of 1934 "
         "GROSVENOR REGISTERED MULTI-STRATEGY FUND (TI 2), LLC (Name of Issuer) "
         "GROSVENOR REGISTERED MULTI-STRATEGY FUND (TI 2), LLC (Name of Person(s) Filing Statement) "
         "SHARES OF LIMITED LIABILITY COMPANY INTEREST (Title of Class of Securities) N/A (CUSIP Number of Class of Securities)")
    c = tender.cover(t)
    assert c["subject_company"] == "GROSVENOR REGISTERED MULTI-STRATEGY FUND (TI 2), LLC"
    assert c["filer"] == "GROSVENOR REGISTERED MULTI-STRATEGY FUND (TI 2), LLC"
    assert c["security_class"] == "SHARES OF LIMITED LIABILITY COMPANY INTEREST"


def test_stake_13d_amendment():
    # SCHEDULE 13D/A 0001493152-26-042923 (Boost Run, Amendment No. 2; Items 6-7 trimmed from the fixture)
    r = stake.extract("SCHEDULE 13D/A", [(1, "SCHEDULE 13D/A", fixture("stake_brun_000149315226042923.txt"))])
    assert r["issuer"] == "Boost Run Inc."
    assert r["cusip"] == "09940T100"
    assert r["lead_filer"] == "Sean Goodrich" and r["filers"] == 2
    assert r["pct"] == 8.88 and r["shares"] == 4437921.0
    assert r["amendment_no"] == 2 and r["event_date"] == dt.date(2026, 9, 14)
    assert r["funds"] == "PF" and r["purpose_flags"] == "investment_only"


def test_stake_picks_cover_page_over_xml_twin():
    # New-format SCHEDULE 13Ds store primary_doc.html and primary_doc.xml under the same seq; the XML text has values
    # but no labels. Before v3 whichever came first was read, blanking issuer and pct on about half these filings.
    xml = ("X0202 SCHEDULE 13D/A 0001224962 XXXXXXXX LIVE 2 Common Stock 09/14/2026 false 0001808805 09940T100 "
           "Boost Run Inc. Sean Goodrich PF N X1 4437921.00 0 4437921.00 0 4437921.00 N 8.88 IN")
    html = fixture("stake_brun_000149315226042923.txt")
    for order in ([(1, "SCHEDULE 13D/A", xml), (1, "SCHEDULE 13D/A", html)], [(1, "SCHEDULE 13D/A", html), (1, "SCHEDULE 13D/A", xml)]):
        r = stake.extract("SCHEDULE 13D/A", order)
        assert r["issuer"] == "Boost Run Inc." and r["cusip"] == "09940T100" and r["pct"] == 8.88


def test_tender_common_plus_preferred_is_not_debt():
    # Care.com 2020 (IAC's Buzz Merger Sub): the offer covers common and convertible preferred at $15 a share
    cov = dict(security_class="Common Stock, par value $0.001 per share (\"Common Shares\") Series A Convertible "
                              "Preferred Stock, $0.001 par value per share (\"Preferred Shares\")", filer="Buzz Merger Sub Inc.")
    trm = dict(at_nav=False, dutch_auction=False)
    text = "Buzz Merger Sub Inc., a Delaware corporation and a wholly-owned subsidiary of IAC/InterActiveCorp"
    assert tender.classify("SC TO-T", cov, trm, text) == "acquisition"
    cov2 = dict(security_class="5.50% Convertible Debentures due 2038 (Series B)", filer="X")
    assert tender.classify("SC TO-I", cov2, trm, "") == "debt_or_preferred"


def test_filing_index_rows():
    # b192: one daily-index line and one quarterly full-index line -> sec.filing_index rows
    import re
    feeds = load("cloud/feeds/main.py", "feeds_main_idx")
    ref = load("cloud/feeds/sec_reference.py", "sec_reference_idx")
    daily = "4                Smith John                                        1234567     20261028    edgar/data/1234567/0001234567-26-000012.txt"
    qtr = "8-K              APPLE INC                                         320193      2026-07-31  edgar/data/320193/0000320193-26-000071.txt"
    r1 = feeds.index_row(ref.IDX_LINE.match(daily), "daily", "2026-10-29 12:00:00")
    assert r1["form"] == "4" and r1["cik"] == "1234567" and r1["date_filed"] == "2026-10-28"
    assert r1["accession"] == "0001234567-26-000012" and r1["acc18"] == "000123456726000012"
    r2 = feeds.index_row(ref.IDX_LINE.match(qtr), "quarterly", "2026-10-29 12:00:00")
    assert r2["company"] == "APPLE INC" and r2["date_filed"] == "2026-07-31" and r2["source"] == "quarterly"
