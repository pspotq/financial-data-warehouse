"""Build every view of the warehouse, then table descriptions and DATA_DICTIONARY.md. Safe to re-run.

  python build.py      (reads the project id from warehouse/config.json: {"project": "<gcp-project-id>"})

Layout:
  sec, gov, markets   raw tables loaded by the Cloud Run ingest jobs (cloud/feeds, cloud/market, cloud/gov)
  ops                 the jobs' run logs          staging   the jobs' MERGE scratch (expires daily)
  rrp                 the reading layer: every view, named by subject (sec_, insider_, crypto_, stablecoins_, equity_,
                      macro_, yields_, gov_, momentum_, health_)
All view SQL lives in this file. Views are built in the order listed (a view may read the ones above it).
Every view in rrp that is not listed here is deleted, so this file is the full list.
"""
import datetime as dt, json, os

from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, Forbidden, NotFound

from catalog import CATALOG, DATASETS

HERE = os.path.dirname(os.path.abspath(__file__))
P = json.load(open(os.path.join(HERE, "config.json")))["project"]
bq = bigquery.Client(project=P)
VIEWS = []   # (dataset.name, sql); `P.` inside backticks becomes the project


def view(name, sql):
    VIEWS.append((name, sql))


FUNCTIONS = []   # (dataset.name, "(arg TYPE, ...)", sql): table functions, for reads that must filter before computing


def table_function(name, args, sql):
    FUNCTIONS.append((name, args, sql))


view("rrp.health_feeds", """-- latest result per rrp-feeds source; stale = no successful run in 3 hours (dod, tickers, delist and the
-- tender / stake / redemption term extractors and funds (Form D + 13F, added 2026-10-23), and since 2026-10-26 form4
-- (overnight daily index), cot (Fridays + a daily catch-up), war_rss (twice a day) and cusip_figi (08:05 UTC, added
-- 2026-11-07) - daily: 30 hours;
-- xbrl - Mondays: 8 days), and TRUE when there is no successful run in the window at all. last_error is the latest run's
-- error only (NULL once a feed recovers); fails_48h counts failed runs in the window (2026-10-19).
WITH r AS (SELECT * FROM `P.ops.feeds_runs` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 9 DAY) AND source NOT LIKE 'kalshi%'
           AND source NOT LIKE '%_history'   -- one-off backfills (efts_history) are not health sources
           AND (ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY) OR source = 'xbrl'))
SELECT source, MAX(ts) AS last_run, MAX(IF(ok, ts, NULL)) AS last_ok, ROUND(AVG(IF(ok, 1, 0)), 3) AS ok_rate_48h,
       ARRAY_AGG(note IGNORE NULLS ORDER BY ts DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_note,
       IF(ARRAY_AGG(ok ORDER BY ts DESC LIMIT 1)[OFFSET(0)], NULL, ARRAY_AGG(error IGNORE NULLS ORDER BY ts DESC LIMIT 1)[SAFE_OFFSET(0)]) AS last_error,
       IFNULL(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(IF(ok, ts, NULL)), MINUTE) >
         CASE WHEN source IN ('dod', 'tickers', 'delist', 'tender_terms', 'stake_terms', 'redemption_terms', 'funds',
                              'form4', 'cot', 'war_rss', 'edgar_index', 'cusip_figi') THEN 1800
              WHEN source = 'xbrl' THEN 11520 ELSE 180 END, TRUE) AS stale,
       COUNTIF(NOT ok) AS fails_48h
FROM r GROUP BY source""")
view("rrp.health_market", """-- latest result per rrp-market source plus 24h success rate. stale = no successful pull within the source's window:
-- 75 min for the 30-minute sources, 90 min for hourly sources (mempool included), 26 hours for the daily funding pull, 4 days for the
-- once-a-weekday equities pass and its symbol directory. One-off backfills (*_history) are left out. stale is TRUE when the
-- window holds no successful pull at all; last_error is the latest run's error only; fails_24h counts failed runs (2026-10-19).
WITH r AS (SELECT * FROM `P.ops.market_runs` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 DAY) AND source NOT LIKE '%_history'),
w AS (SELECT * FROM r WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) OR source IN ('equities', 'universe', 'short_volume', 'short_interest', 'treasury_dts', 'eia', 'eia_weekly', 'degree_days', 'etf_shares'))
SELECT source, MAX(ts) AS last_run, MAX(IF(ok, ts, NULL)) AS last_ok,
       ROUND(AVG(IF(ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY), IF(ok, 1, 0), NULL)), 3) AS ok_rate_24h,
       COUNTIF(ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS runs_24h,
       IF(ARRAY_AGG(ok ORDER BY ts DESC LIMIT 1)[OFFSET(0)], NULL, ARRAY_AGG(error IGNORE NULLS ORDER BY ts DESC LIMIT 1)[SAFE_OFFSET(0)]) AS last_error,
       IFNULL(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(IF(ok, ts, NULL)), MINUTE) >
         CASE WHEN source IN ('equities', 'universe', 'short_volume', 'short_interest', 'treasury_dts', 'eia', 'eia_weekly', 'degree_days', 'etf_shares') THEN 4 * 1440
              WHEN source = 'funding' THEN 1560
              WHEN source IN ('crypto_market', 'farside_etf', 'defillama_stablecoins', 'fear_greed', 'macro_fred', 'macro_treasury', 'macro_cboe', 'mempool') THEN 90
              ELSE 75 END, TRUE) AS stale,
       COUNTIF(NOT ok AND ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS fails_24h
FROM w WHERE source NOT IN ('macro_lbma', 'cef_nav', 'macro_cboe') GROUP BY source""")
view("rrp.filing_index", """-- every EDGAR filing, one row per filing (b192 / b193, added 2026-10-29): EDGAR's own form index (daily and the quarterly
-- full index back to 1993) with each filing's parties, plus the full-text-search streams that matched it as tags.
-- parties = every CIK the index lists the filing under (filer, subject, reporting owners). streams = the rrp-sec
-- full-text streams that hit it ([] when none; a filing can match several), items = the 8-K item numbers when a stream
-- saw it. Filter on date_filed: the source tables are partitioned on it, so a date range keeps reads small.
WITH idx AS (
  SELECT acc18, date_filed, ANY_VALUE(accession) AS accession, ANY_VALUE(form) AS form,
         ARRAY_AGG(STRUCT(cik, company) ORDER BY cik) AS parties
  FROM `P.sec.filing_index` GROUP BY acc18, date_filed),
tags AS (
  SELECT REPLACE(accession, '-', '') AS acc18, file_date, ARRAY_AGG(DISTINCT query_id ORDER BY query_id) AS streams,
         ANY_VALUE(items) AS items
  FROM `P.sec.filing_hits` GROUP BY 1, 2)
SELECT i.acc18, i.accession, i.form, i.date_filed, i.parties, ARRAY_LENGTH(i.parties) AS n_parties,
       IFNULL(t.streams, []) AS streams, t.items,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', i.parties[OFFSET(0)].cik, '/', i.acc18, '/') AS index_url
FROM idx i LEFT JOIN tags t ON t.acc18 = i.acc18 AND t.file_date = i.date_filed""")

view("rrp.sec_filings", """-- one row per filing per EDGAR full-text-search stream, newest first. Primary documents only (file_type equals
-- the form) unless the stream only matches exhibits; doc_url opens the matched document, index_url the filing index.
SELECT query_id AS stream, file_date, form, accession, display_names AS filer, ciks, items, file_type,
       file_description, biz_locations, index_url, doc_url, first_seen
FROM `P.sec.filing_hits` WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY query_id, accession, file_date ORDER BY (file_type = form) DESC, first_seen) = 1""")
# file_date is in the PARTITION BY (it never varies within a filing) so a WHERE on file_date prunes sec.filing_hits
# partitions through this view: the table holds stream history back to 2001 (backfill added 2026-10-19).
view("rrp.sec_filings_new", """-- filings first seen in the last 26 hours AND filed in the last 3 days: the working list for the morning and sweep runs
SELECT * FROM `P.rrp.sec_filings`
WHERE first_seen > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 26 HOUR)
  AND file_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 DAY)""")
view("rrp.sec_docs", """-- filing text: one row per document, newest fetch per document. doc_type INDEX = the filing's document list
-- (seq | type | description | name | size); FORM4_XML = a Form 4's ownership XML; otherwise the EDGAR document type
-- (8-K, SC TO-I, SCHEDULE 13D, EX-99.1, EX-2.1 ...). Filter on acc18 (18-digit accession) AND a file_date range: since
-- 2026-10-29 sec.filing_docs requires a file_date filter (a read without one fails; one by acc18 alone billed ~16 GB).
SELECT accession, acc18, form, file_date, cik, seq, doc_type, description, doc_name, url, size_bytes, chars, truncated, text, source, fetched_at
FROM `P.sec.filing_docs` WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18, file_date, doc_name ORDER BY fetched_at DESC) = 1""")
# file_date is in the PARTITION BY (constant within a filing) so a WHERE on file_date prunes sec.filing_docs partitions
# through this view: the table holds filing text back to 2015 (history fill added 2026-10-20), so an unpruned read is GBs.

view("rrp.tender_terms", """-- Schedule TO terms as columns (sec.tender_terms, rule-based extraction from the filing text; b170). One row per
-- filing, newest extractor version. offer_kind: acquisition (merger sub), mini_tender (MacKenzie, Comrit, Everest ...),
-- third_party_other, fund_repurchase (interval funds, BDCs, non-traded REITs: usually at NAV), dutch_auction,
-- fixed_price, exchange_offer, warrant_tender, option_exchange. Amendments (form ending /A) carry shares_tendered,
-- shares_accepted, final_price, new_expiration_date, is_final. Hand check (65 filings): price, expiration and is_final
-- right 93-100%; size (max_*) right about half the time. debt_or_preferred / exchange offers carry no per-share price.
SELECT * EXCEPT (extracted_at) FROM `P.sec.tender_terms` WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18 ORDER BY extractor_version DESC, extracted_at DESC) = 1""")
view("rrp.tender_offers", """-- one row per tender offer: the original Schedule TO with what its amendments added (same filer directory CIK and
-- form family, filed within 180 days after, before the next original): latest expiration, final counts, final price
WITH t AS (SELECT * FROM `P.rrp.tender_terms`),
o AS (SELECT *, LEAD(file_date) OVER (PARTITION BY cik, form ORDER BY file_date, acc18) AS next_original
      FROM t WHERE NOT ENDS_WITH(form, '/A')),
a AS (SELECT o.acc18 AS orig, t.* FROM o JOIN t ON t.cik = o.cik AND t.form = CONCAT(o.form, '/A')
        AND t.file_date >= o.file_date AND t.file_date <= DATE_ADD(o.file_date, INTERVAL 180 DAY)
        AND (o.next_original IS NULL OR t.file_date < o.next_original)),
agg AS (SELECT orig, COUNT(*) AS amendments, MAX(file_date) AS last_amended,
          ARRAY_AGG(new_expiration_date IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS extended_to,
          ARRAY_AGG(shares_tendered IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS shares_tendered,
          ARRAY_AGG(shares_accepted IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS shares_accepted,
          ARRAY_AGG(final_price IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS final_price,
          LOGICAL_OR(is_final) AS final_reported
        FROM a GROUP BY orig)
SELECT o.acc18, o.accession, o.form, o.file_date, o.cik, o.subject_company, o.filer, o.security_class, o.cusip, o.offer_kind,
       o.price_per_share, o.price_low, o.price_high, o.at_nav, o.dutch_auction, o.max_shares, o.max_pct, o.max_amount_usd,
       o.transaction_value_usd, o.expiration_date, agg.extended_to, COALESCE(agg.extended_to, o.expiration_date) AS last_expiration,
       IFNULL(agg.amendments, 0) AS amendments, agg.last_amended, agg.shares_tendered, agg.shares_accepted, agg.final_price,
       IFNULL(agg.final_reported, FALSE) AS final_reported,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', o.cik, '/', o.acc18, '/', o.accession, '-index.htm') AS index_url
FROM o LEFT JOIN agg ON agg.orig = o.acc18""")

view("rrp.stakes", """-- Schedule 13D stakes as columns (sec.stake_terms, rule-based extraction from the filing text; b171). One row per
-- filing (SC 13D and, from Dec 2024, SCHEDULE 13D, with amendments), newest extractor version. pct / shares / top_filer
-- = the largest stake on the cover page; persons = every reporting person (name, funds, shares, pct, type);
-- purpose_flags = keywords in Item 4: board_seat, proxy_fight, sale_or_merger, letter_to_board, cooperation_agreement,
-- investment_only. funds codes: WC working capital, PF personal funds, AF affiliate, OO other, BK bank.
SELECT * EXCEPT (extracted_at) FROM `P.sec.stake_terms` WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18 ORDER BY extractor_version DESC, extracted_at DESC) = 1""")
view("rrp.stake_changes", """-- every 13D filing with the previous filing's stake for the same issuer (CUSIP, else name) and lead filer:
-- prev_pct, pct_change, first_filing (no earlier one), crossed_up / crossed_down 10% and 20%, exited (pct now 0)
WITH s AS (SELECT acc18, accession, form, file_date, event_date, issuer, cusip, lead_filer, top_filer, filers, pct, shares,
                  funds, filer_types, purpose_flags, amendment_no,
                  COALESCE(cusip, UPPER(REGEXP_REPLACE(issuer, r'[^A-Za-z0-9]', ''))) AS issuer_key,
                  UPPER(REGEXP_REPLACE(lead_filer, r'[^A-Za-z0-9]', '')) AS filer_key
           FROM `P.rrp.stakes` WHERE pct IS NOT NULL),
l AS (SELECT *, LAG(pct) OVER w AS prev_pct, LAG(file_date) OVER w AS prev_date, LAG(shares) OVER w AS prev_shares
      FROM s WINDOW w AS (PARTITION BY issuer_key, filer_key ORDER BY file_date, acc18))
SELECT * EXCEPT (issuer_key, filer_key), ROUND(pct - prev_pct, 2) AS pct_change, prev_pct IS NULL AS first_filing,
       (prev_pct < 10 AND pct >= 10) OR (prev_pct < 20 AND pct >= 20) AS crossed_up,
       (prev_pct >= 10 AND pct < 10) OR (prev_pct >= 20 AND pct < 20) AS crossed_down,
       pct = 0 AND IFNULL(prev_pct, 0) > 0 AS exited
FROM l""")

view("rrp.redemptions", r"""-- calls and redemptions announced in 8-Ks (sec.redemption_terms, rule-based; b172): one row per filing that announces
-- a call of the issuer's own notes, preferred or warrants, newest extractor version. security / security_kind / full_call /
-- redemption_date / price_pct (% of principal or liquidation preference) / price_per_unit ($ per share or warrant) /
-- make_whole; call_text = the announcing sentence and what follows. Security and date are reliable; amount_usd can be a
-- series' size rather than the amount called - check the filing before quoting it. Ticker from the filer field.
SELECT * EXCEPT (extracted_at, is_call), REGEXP_EXTRACT(filer, r'\(([A-Z]{1,5}(?:[.-][A-Z]{1,2})?)[,)]') AS ticker,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', cik, '/', acc18, '/', accession, '-index.htm') AS index_url
FROM (SELECT * FROM `P.sec.redemption_terms` WHERE TRUE
      QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18 ORDER BY extractor_version DESC, extracted_at DESC) = 1)
WHERE is_call""")

# ------------------------------------------------------------------ insider_*: Form 4 open-market trades
view("rrp.insider_trades", """-- Form 4 open-market buys (P) and sales (S), one row per distinct transaction line, every EDGAR day since Jan 2006.
-- source 'sec_dataset' (2006-2020, SEC quarterly data sets: shares and prices rounded to 2 decimals); 'edgar_daily_index'
-- (2021 on, parsed from each filing: exact).
-- A jointly filed Form 4 lists the same trade once per filer: collapsed to one (copies = how many).
-- An amended Form 4 (4/A) line identical to a line of an original Form 4 (same ticker, insider, transaction date, code,
-- shares and price) is left out; a 4/A line that changes anything is kept as a correction.
-- value_suspect: the filer's own numbers are implausible (a line over $2B, price equal to shares, a price over $1M).
-- Kept as filed; leave it out of any sum, record or "largest" comparison. value is negative for sales.
SELECT * EXCEPT(rn, originals),
  IFNULL(ABS(value) > 2e9 OR (price = shares AND shares >= 100000) OR price > 1e6, FALSE) AS value_suspect
FROM (
  SELECT filed, tx_date, ticker, issuer, insider, role, code, shares, price, value, owned_after, direct_indirect,
    plan_10b5_1, private_placement, non_usd_suspect, accession, footnotes, kind, source, form, issuer_cik, security,
    COUNT(*) OVER w AS copies, ROW_NUMBER() OVER (w ORDER BY direct_indirect) AS rn,
    COUNTIF(form = '4') OVER (PARTITION BY ticker, insider, tx_date, code, CAST(shares AS STRING), CAST(price AS STRING)) AS originals
  FROM `P.sec.form4`
  WINDOW w AS (PARTITION BY accession, ticker, insider, tx_date, code, CAST(shares AS STRING), CAST(price AS STRING),
               CAST(owned_after AS STRING))
) WHERE rn = 1 AND NOT (form = '4/A' AND originals > 0)""")
view("rrp.insider_buys", r"""-- open-market Form 4 purchases with the noise removed: real tickers only, no 10b5-1 plan, private placement, ESPP,
-- DSU/fee-in-lieu grant, DRIP or foreign parallel-offering lines, no amendments, no implausible values, $10K minimum
SELECT * FROM `P.rrp.insider_trades`
WHERE code = 'P' AND form = '4' AND NOT value_suspect
  AND REGEXP_CONTAINS(ticker, r'^[A-Z]{1,5}([.-][A-Z])?$') AND ticker NOT IN ('NONE', 'NA')
  AND NOT IFNULL(plan_10b5_1, FALSE) AND NOT IFNULL(private_placement, FALSE) AND NOT IFNULL(non_usd_suspect, FALSE)
  AND value >= 10000
  AND NOT REGEXP_CONTAINS(LOWER(IFNULL(footnotes, '')),
      r'employee stock purchase|espp|deferred stock unit|\bdsus?\b|dividend reinvest|\bdrip\b|grant date fair value|in lieu of cash|director fees|settlement date, which is the date the price per share|net asset value|nav per share|parallel offering|euro |€|brl|r\$')""")

# ------------------------------------------------------------------ sec reference: company facts, tickers, delistings (added 2026-10-12)
view("rrp.sec_xbrl_facts", """-- company facts from 10-K / 10-Q / 20-F filings (SEC XBRL): one row per company, concept, unit, period and filing.
-- filed is the filing date (NULL on rows from the weekly frames refresh); use only rows filed before the date you study.
-- Rows with an impossible period end (before EDGAR, or more than 400 days after filing, e.g. 2215) are left out: ~570 of 14.7M.
SELECT cik, entity, taxonomy, concept, unit, period_start, period_end, val, accn, fy, fp, form, filed, frame, source
FROM `P.sec.xbrl_facts`
WHERE period_end BETWEEN DATE '1993-01-01' AND DATE_ADD(COALESCE(filed, CURRENT_DATE()), INTERVAL 400 DAY)""")
view("rrp.sec_shares_outstanding", """-- shares outstanding per company per report, point in time: the cover-page figure (dei) when reported, else the
-- balance-sheet figure. known_from = the filing date (frames rows, which carry none, use period end + 45 days).
-- Companies with several share classes may show one class only.
SELECT cik, entity, period_end, val AS shares, concept, accn, form, filed,
       COALESCE(filed, DATE_ADD(period_end, INTERVAL 45 DAY)) AS known_from
FROM `P.sec.xbrl_facts`
WHERE unit = 'shares' AND concept IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding') AND val > 0
  AND period_end BETWEEN DATE '1993-01-01' AND DATE_ADD(COALESCE(filed, CURRENT_DATE()), INTERVAL 400 DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY cik, period_end, accn
                           ORDER BY concept = 'EntityCommonStockSharesOutstanding' DESC, source = 'companyfacts' DESC) = 1""")
view("rrp.sec_company_latest", """-- one row per company: the newest reported value of each balance-sheet item (US$ unless noted) and its period end
WITH f AS (
  SELECT cik, concept, ARRAY_AGG(STRUCT(val, period_end, entity) ORDER BY period_end DESC, filed DESC LIMIT 1)[OFFSET(0)] AS x
  FROM `P.sec.xbrl_facts` WHERE period_start IS NULL AND unit IN ('USD', 'shares')
    AND period_end BETWEEN DATE '1993-01-01' AND DATE_ADD(COALESCE(filed, CURRENT_DATE()), INTERVAL 400 DAY) GROUP BY cik, concept)
SELECT cik, ANY_VALUE(x.entity) AS entity,
  COALESCE(MAX(IF(concept = 'EntityCommonStockSharesOutstanding', x.val, NULL)), MAX(IF(concept = 'CommonStockSharesOutstanding', x.val, NULL))) AS shares_outstanding,
  MAX(IF(concept = 'EntityPublicFloat', x.val, NULL)) AS public_float,
  COALESCE(MAX(IF(concept = 'CashAndCashEquivalentsAtCarryingValue', x.val, NULL)),
           MAX(IF(concept = 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents', x.val, NULL))) AS cash,
  COALESCE(MAX(IF(concept = 'ShortTermInvestments', x.val, NULL)), MAX(IF(concept = 'MarketableSecuritiesCurrent', x.val, NULL))) AS short_term_investments,
  MAX(IF(concept = 'AssetsCurrent', x.val, NULL)) AS current_assets, MAX(IF(concept = 'Assets', x.val, NULL)) AS total_assets,
  MAX(IF(concept = 'LiabilitiesCurrent', x.val, NULL)) AS current_liabilities, MAX(IF(concept = 'Liabilities', x.val, NULL)) AS total_liabilities,
  COALESCE(MAX(IF(concept = 'LongTermDebt', x.val, NULL)), MAX(IF(concept = 'LongTermDebtNoncurrent', x.val, NULL))) AS long_term_debt,
  COALESCE(MAX(IF(concept = 'DebtCurrent', x.val, NULL)), MAX(IF(concept = 'LongTermDebtCurrent', x.val, NULL))) AS debt_current,
  MAX(IF(concept = 'StockholdersEquity', x.val, NULL)) AS equity,
  MAX(IF(concept IN ('Assets', 'StockholdersEquity', 'Liabilities'), x.period_end, NULL)) AS balance_sheet_as_of
FROM f GROUP BY cik""")
view("rrp.cik_tickers", """-- every ticker the SEC has mapped to a company since the daily snapshots began, with first and last day seen;
-- is_current = on the newest list (a delisted company drops off the SEC list but stays here)
WITH t AS (SELECT cik, ticker, ANY_VALUE(name) AS name, ANY_VALUE(exchange) AS exchange, MIN(as_of) AS first_seen, MAX(as_of) AS last_seen
           FROM `P.sec.cik_tickers` GROUP BY cik, ticker)
SELECT *, last_seen = (SELECT MAX(as_of) FROM `P.sec.cik_tickers`) AS is_current FROM t""")
view("rrp.sec_delistings", r"""-- Form 25 / 25-NSE (an exchange removing a security from listing) and Form 15 / 15F (deregistration) filings by company,
-- newest first. kind: delisting or deregistration. A Form 25 can cover notes or preferred shares rather than the common
-- stock; open url when it matters. ticker = the issuer's most recent Form 4 ticker, when it has one.
-- A Form 25-NSE is indexed under both the exchange that files it and the company; the exchange's rows (NYSE, Nasdaq,
-- NYSE Arca / American, Cboe BZX and older venues, by CIK) are left out (2026-10-26), so each row is a company.
WITH d AS (SELECT * FROM `P.sec.delistings`
           WHERE cik NOT IN ('876661', '1354457', '1143362', '1143313', '1417835', '1296945', '876882', '876798')   -- NYSE, Nasdaq,
             -- NYSE Arca, NYSE American (ex-AMEX), Cboe BZX (ex-BATS), Boston SE, NYSE Chicago / Texas, Philadelphia SE
           QUALIFY ROW_NUMBER() OVER (PARTITION BY accession, cik ORDER BY form) = 1),
t AS (SELECT issuer_cik AS cik, ARRAY_AGG(ticker ORDER BY filed DESC LIMIT 1)[OFFSET(0)] AS ticker
      FROM `P.sec.form4` WHERE REGEXP_CONTAINS(ticker, r'^[A-Z]{1,5}([.-][A-Z])?$') GROUP BY issuer_cik)
SELECT d.cik, d.company, t.ticker, d.form, IF(STARTS_WITH(d.form, '25'), 'delisting', 'deregistration') AS kind, d.filed,
       d.accession, CONCAT('https://www.sec.gov/Archives/', d.path) AS url
FROM d LEFT JOIN t USING (cik)""")
view("rrp.insider_buys_sized", """-- rrp.insider_buys with company size at the time of the buy: shares outstanding last reported before the buy, the
-- close on or before the buy date (the buy's own price before Sep 2021, where equity_daily starts), market cap, the
-- buy as a share of market cap and of shares outstanding, and the first Form 25 / 15 filed after the buy (delisted_on)
WITH b AS (SELECT *, FARM_FINGERPRINT(CONCAT(accession, '|', insider, '|', CAST(tx_date AS STRING), '|', CAST(shares AS STRING), '|',
                                         IFNULL(CAST(price AS STRING), ''))) AS bid FROM `P.rrp.insider_buys`),
sh AS (SELECT b.bid, s.shares AS shares_out, s.period_end AS shares_as_of
       FROM b JOIN `P.rrp.sec_shares_outstanding` s
         ON s.cik = b.issuer_cik AND s.known_from <= b.tx_date AND s.period_end >= DATE_SUB(b.tx_date, INTERVAL 400 DAY)
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY b.bid ORDER BY s.known_from DESC, s.period_end DESC) = 1),
px AS (SELECT b.bid, e.close AS close_at_buy, e.date AS close_date
       FROM b JOIN `P.markets.equity_daily` e ON e.symbol = b.ticker AND e.date BETWEEN DATE_SUB(b.tx_date, INTERVAL 7 DAY) AND b.tx_date
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY b.bid ORDER BY e.date DESC) = 1),
dl AS (SELECT b.bid, d.filed AS delisted_on, d.form AS delist_form
       FROM b JOIN `P.rrp.sec_delistings` d ON d.cik = b.issuer_cik AND d.filed > b.tx_date
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY b.bid ORDER BY d.filed) = 1)
SELECT b.* EXCEPT(bid), sh.shares_out, sh.shares_as_of, px.close_at_buy, px.close_date,
  ROUND(sh.shares_out * COALESCE(px.close_at_buy, b.price) / 1e6, 2) AS mkt_cap_usd_m,
  ROUND(100 * b.value / NULLIF(sh.shares_out * COALESCE(px.close_at_buy, b.price), 0), 4) AS buy_pct_of_mkt_cap,
  ROUND(100 * b.shares / NULLIF(sh.shares_out, 0), 4) AS buy_pct_of_shares,
  dl.delisted_on, dl.delist_form
FROM b LEFT JOIN sh USING (bid) LEFT JOIN px USING (bid) LEFT JOIN dl USING (bid)""")

# ------------------------------------------------------------------ short_*: FINRA short data (added 2026-10-23, b166)
view("rrp.short_interest", """-- FINRA short interest, twice a month (settlement dates mid-month and month end; published about 7 business days later).
-- Every exchange-listed and OTC stock from June 2021; OTC only before that (history from Dec 2017). short_qty = shares sold
-- short and not yet covered at settlement; change_qty / change_pct vs the prior settlement date as FINRA reports it;
-- days_to_cover = short_qty / avg_daily_vol (FINRA's figure). pct_of_shares_out = short_qty over the shares outstanding last
-- reported to the SEC before the settlement date (XBRL cover page; not float: float in shares is not reported).
-- cik comes from the SEC ticker map (latest company for the ticker; reused tickers can map to the newer company).
WITH tk AS (SELECT ticker, ARRAY_AGG(cik ORDER BY last_seen DESC LIMIT 1)[OFFSET(0)] AS cik FROM `P.rrp.cik_tickers` GROUP BY ticker),
si AS (SELECT s.*, tk.cik FROM `P.markets.short_interest` s LEFT JOIN tk ON tk.ticker = REPLACE(s.symbol, '.', '-')
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY s.settlement_date, s.symbol ORDER BY s.ingested_at DESC) = 1),
sh AS (SELECT si.settlement_date, si.symbol, o.shares, o.period_end
       FROM si JOIN `P.rrp.sec_shares_outstanding` o
         ON o.cik = si.cik AND o.known_from <= si.settlement_date AND o.period_end >= DATE_SUB(si.settlement_date, INTERVAL 400 DAY)
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY si.settlement_date, si.symbol ORDER BY o.known_from DESC, o.period_end DESC) = 1)
SELECT si.settlement_date, si.symbol, si.name, si.market, si.cik, si.short_qty, si.prev_short_qty,
       si.short_qty - si.prev_short_qty AS change_qty, si.change_pct, si.avg_daily_vol, si.days_to_cover,
       sh.shares AS shares_out, sh.period_end AS shares_as_of,
       ROUND(100 * si.short_qty / NULLIF(sh.shares, 0), 2) AS pct_of_shares_out,
       si.split_flag, si.revision
FROM si LEFT JOIN sh USING (settlement_date, symbol)""")
view("rrp.short_volume", """-- FINRA Reg SHO daily short sale volume (consolidated NMS file), every trading day from Aug 2018, one row per symbol.
-- CAUTION: total_volume is only the volume reported to FINRA's trade reporting facilities (mostly off-exchange), NOT the
-- stock's full consolidated volume, and short_volume counts sales marked short, including market makers' routine
-- hedging. short_pct near 40-50% is normal; it is not short interest. Compare a stock against its own history
-- (short_pct_20d_avg), never across stocks. Published each trading evening; loaded by the weekday 22:20 UTC pass.
SELECT date, symbol, short_volume, short_exempt_volume, total_volume, market,
       ROUND(100 * SAFE_DIVIDE(short_volume, total_volume), 2) AS short_pct,
       ROUND(100 * SAFE_DIVIDE(SUM(short_volume) OVER w, SUM(total_volume) OVER w), 2) AS short_pct_20d_avg
FROM `P.markets.short_volume`
WINDOW w AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)""")

# ------------------------------------------------------------------ treasury_*: Daily Treasury Statement (added 2026-10-23, b165)
view("rrp.treasury_daily", """-- one row per business day from the Daily Treasury Statement (Oct 2005 on), $ millions. tga_close = the Treasury's
-- cash at the Fed (Federal Reserve Account before Oct 2021, Treasury General Account after; DTS moved the closing figure
-- into its 'today' column in Apr 2022, handled here). tga_change = vs the prior business day. debt_issues / debt_redemptions
-- = public debt cash issues and redemptions (Table IIIB); net_debt_issuance = issues - redemptions. tax_receipts = every
-- 'Taxes - ...' deposit line, which exists only from Feb 14 2023, when Treasury redesigned the statement (NULL before).
WITH c AS (
  SELECT record_date,
    COALESCE(MAX(IF(account_type = 'Treasury General Account (TGA) Closing Balance', open_today_bal, NULL)),
             MAX(IF(account_type = 'Treasury General Account (TGA)', close_today_bal, NULL)),
             MAX(IF(account_type = 'Federal Reserve Account', close_today_bal, NULL))) AS tga_close,
    MAX(IF(account_type = 'Tax and Loan Note Accounts (Table V)', close_today_bal, NULL)) AS tax_and_loan_close
  FROM `P.gov.treasury_dts` WHERE table_id = 'cash' GROUP BY record_date),
f AS (
  SELECT record_date,
    SUM(IF(transaction_type = 'Deposits' AND STARTS_WITH(category, 'Public Debt Cash Issues'), today_amt, 0)) AS debt_issues,
    SUM(IF(transaction_type = 'Withdrawals' AND STARTS_WITH(category, 'Public Debt Cash Redemp'), today_amt, 0)) AS debt_redemptions,
    IF(record_date >= DATE '2023-02-14',
       SUM(IF(transaction_type = 'Deposits' AND STARTS_WITH(category, 'Taxes - '), today_amt, 0)), NULL) AS tax_receipts,
    IF(record_date >= DATE '2023-02-14',
       SUM(IF(transaction_type = 'Deposits' AND category = 'Taxes - Withheld Individual/FICA', today_amt, 0)), NULL) AS withheld_taxes,
    IF(record_date >= DATE '2023-02-14',
       SUM(IF(transaction_type = 'Deposits' AND category = 'Taxes - Corporate Income', today_amt, 0)), NULL) AS corporate_taxes,
    SUM(IF(transaction_type = 'Deposits' AND REGEXP_CONTAINS(category, r'Customs'), today_amt, 0)) AS customs_receipts
  FROM `P.gov.treasury_dts` WHERE table_id = 'flows' AND category IS NOT NULL GROUP BY record_date)
SELECT record_date, tga_close, tga_close - LAG(tga_close) OVER (ORDER BY record_date) AS tga_change, tax_and_loan_close,
       debt_issues, debt_redemptions, debt_issues - debt_redemptions AS net_debt_issuance,
       tax_receipts, withheld_taxes, corporate_taxes, customs_receipts
FROM c LEFT JOIN f USING (record_date)""")
view("rrp.treasury_debt_limit", """-- federal debt against the statutory limit per business day (DTS Table IIIC), $ millions, Oct 2005 on.
-- subject_to_limit = held by the public + intragovernmental - debt not subject to the limit + other debt subject to it.
-- statutory_limit is NULL while the limit is suspended (limit_suspended = TRUE); headroom = limit - subject_to_limit.
WITH d AS (
  SELECT record_date,
    SUM(IF(category = 'Debt Held by the Public', close_today_bal, 0)) AS held_by_public,
    SUM(IF(category = 'Intragovernmental Holdings', close_today_bal, 0)) AS intragovernmental,
    SUM(IF(REGEXP_CONTAINS(category, r'Not Subject to Limit'), close_today_bal, 0)) AS not_subject,
    SUM(IF(REGEXP_CONTAINS(category, r'Other Debt Subject to Limit'), close_today_bal, 0)) AS other_subject,
    MAX(IF(category = 'Statutory Debt Limit', close_today_bal, NULL)) AS statutory_limit
  FROM `P.gov.treasury_dts` WHERE table_id = 'debt_limit' GROUP BY record_date)
SELECT record_date, held_by_public, intragovernmental, held_by_public + intragovernmental AS total_public_debt,
       held_by_public + intragovernmental - not_subject + other_subject AS subject_to_limit, statutory_limit,
       statutory_limit IS NULL AS limit_suspended,
       statutory_limit - (held_by_public + intragovernmental - not_subject + other_subject) AS headroom
FROM d""")
view("rrp.treasury_flows", """-- every Daily Treasury Statement deposit and withdrawal line (Oct 2005 on), $ millions: one row per day, account,
-- direction and category. Category names changed in Oct 2021 and Feb 2023 (e.g. 'Taxes - Corporate Income' from Feb 2023);
-- compare within one naming era. Pre-2023 withheld and corporate taxes are in rrp.treasury_tax_deposits.
SELECT record_date, account_type AS account, transaction_type AS direction, category, today_amt, mtd_amt, fytd_amt
FROM `P.gov.treasury_dts` WHERE table_id = 'flows' AND category IS NOT NULL""")
view("rrp.treasury_tax_deposits", """-- DTS Table IV federal tax deposits by type, Oct 2005 to Feb 13 2023 (the table was retired in the Feb 2023 redesign;
-- later tax receipts are the 'Taxes - ...' lines of rrp.treasury_flows / tax_receipts in rrp.treasury_daily). $ millions.
SELECT record_date, category AS tax_type, category_desc, today_amt, mtd_amt, fytd_amt
FROM `P.gov.treasury_dts` WHERE table_id = 'tax'""")

# ------------------------------------------------------------------ grid_*: EIA electricity grid monitor (added 2026-10-23, b164)
view("rrp.grid_daily", """-- US electricity per day and balancing authority / region (EIA Form 930), Jan 2019 on, MWh, Eastern-time days.
-- demand, day-ahead forecast, net generation and total interchange (+ = net exports). is_region = US48 (lower 48 total)
-- or one of the 13 EIA regions (CAL, CAR, CENT, FLA, MIDA, MIDW, NE, NY, NW, SE, SW, TEN, TEX); else a single
-- balancing authority (PJM, ERCO, MISO, CISO, ...). demand_yoy_pct = vs the same weekday 52 weeks earlier. The latest day
-- can be revised by EIA for several days.
WITH d AS (
  SELECT period_date AS date, respondent, ANY_VALUE(respondent_name) AS respondent_name,
         MAX(IF(type = 'D', value, NULL)) AS demand_mwh, MAX(IF(type = 'DF', value, NULL)) AS forecast_mwh,
         MAX(IF(type = 'NG', value, NULL)) AS net_generation_mwh, MAX(IF(type = 'TI', value, NULL)) AS interchange_mwh
  FROM `P.gov.eia_grid` WHERE dataset = 'demand' AND frequency = 'daily' GROUP BY 1, 2)
SELECT d.*, d.respondent IN ('US48', 'CAL', 'CAR', 'CENT', 'FLA', 'MIDA', 'MIDW', 'NE', 'NY', 'NW', 'SE', 'SW', 'TEN', 'TEX') AS is_region,
       ROUND(100 * SAFE_DIVIDE(d.demand_mwh - d.forecast_mwh, d.forecast_mwh), 2) AS forecast_miss_pct,
       ROUND(100 * SAFE_DIVIDE(d.demand_mwh - y.demand_mwh, y.demand_mwh), 2) AS demand_yoy_pct
FROM d LEFT JOIN d y ON y.respondent = d.respondent AND y.date = DATE_SUB(d.date, INTERVAL 364 DAY)""")
view("rrp.grid_fuel_mix", """-- US electricity generation by fuel per day and balancing authority / region (EIA Form 930), Jan 2019 on, MWh.
-- share_pct = the fuel's share of that day's total generation (storage can be negative: charging). Battery, geothermal,
-- pumped storage and solar/wind-with-battery are split out only from Nov 2024; before that they sit in other fuels.
WITH f AS (SELECT period_date AS date, respondent, ANY_VALUE(respondent_name) AS respondent_name, type AS fuel,
                  ANY_VALUE(type_name) AS fuel_name, MAX(value) AS mwh
           FROM `P.gov.eia_grid` WHERE dataset = 'fuel' AND frequency = 'daily' GROUP BY 1, 2, 4)
SELECT *, respondent IN ('US48', 'CAL', 'CAR', 'CENT', 'FLA', 'MIDA', 'MIDW', 'NE', 'NY', 'NW', 'SE', 'SW', 'TEN', 'TEX') AS is_region,
       ROUND(100 * SAFE_DIVIDE(mwh, SUM(mwh) OVER (PARTITION BY date, respondent)), 2) AS share_pct
FROM f""")
view("rrp.grid_hourly", """-- hourly US electricity for US48 and the 13 EIA regions, last ~3 days only (rolling; hourly history is not kept).
-- hour_utc = the hour's start in UTC. kind: demand (D demand, DF forecast, NG net generation, TI interchange) or fuel
-- (type = fuel code: NG gas, COL coal, NUC nuclear, SUN solar, WND wind, WAT hydro, BAT battery, OIL, OTH ...). MWh.
SELECT period_ts AS hour_utc, respondent, respondent_name, dataset AS kind, type, type_name, value AS mwh
FROM `P.gov.eia_grid` WHERE frequency = 'hourly'""")

# ------------------------------------------------------------------ formd / form13f: SEC fund filings (added 2026-10-23, b167)
view("rrp.formd", """-- every Form D / D/A (exempt offering notice: private raises, new hedge / PE / VC / crypto funds), 2008 on, one row per
-- filing. new_offering = an original D (not an amendment); amendments update amounts sold and investor counts.
-- fund_type is set for pooled investment funds (Hedge Fund, Private Equity Fund, Venture Capital Fund, Other Investment
-- Fund ...). Amounts in US$; total_offering is NULL when offering_indefinite. exemptions: 06b / 06c = Rule 506(b)/(c),
-- 04 = Rule 504, 3C / 3C.1 / 3C.7 = Investment Company Act exclusions. url opens the filing.
SELECT accession, filed, form, form = 'D' AND IFNULL(is_amendment, FALSE) = FALSE AS new_offering, cik, entity_name,
       entity_type, city, state, jurisdiction, year_of_inc, industry_group, fund_type, is_40act, revenue_range, nav_range,
       exemptions, is_amendment, prev_accession, first_sale_date, first_sale_yet_to_occur, more_than_one_year,
       security_types, min_investment, total_offering, offering_indefinite, total_sold, total_remaining, investors_count,
       has_nonaccredited, nonaccredited_count, sales_commissions, finders_fees, related_persons, source,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', cik, '/', accession, '/') AS url
FROM `P.sec.formd` WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY accession ORDER BY ingested_at DESC) = 1""")
view("rrp.form13f_reports", """-- which 13F filings make up each manager's holdings for each quarter (period), amendments resolved: the base report is
-- the newest 13F-HR or RESTATEMENT amendment (13F-HR/A, amendment_type RESTATEMENT replaces the whole report); NEW
-- HOLDINGS amendments filed after it add lines. 13F-NT (notice: holdings reported by another manager) is left out.
-- role: base / added. report_type COMBINATION = the manager reports some holdings itself and some through others.
-- value_total_usd = the cover-page total as typed (pre-2023 units fixed 2026-10-26, sec.form13f_unit_fixes; a few
-- typos remain, e.g. Dimensional 2022-Q3 ~23x); holdings_value_usd = the sum of the filing's own holding lines
-- (sec.form13f_totals) - use it. Some filers (Norges Bank) file a one-line placeholder 13F-HR and the full table
-- later as a 13F-HR/A: until that arrives the quarter shows ~$0.
WITH f AS (SELECT f.*, t.holdings_value_usd, t.holdings_lines
           FROM `P.sec.form13f_filings` f LEFT JOIN `P.sec.form13f_totals` t USING (accession) WHERE form LIKE '13F-HR%'
           QUALIFY ROW_NUMBER() OVER (PARTITION BY accession ORDER BY ingested_at DESC) = 1),
b0 AS (SELECT * FROM f WHERE form = '13F-HR' OR amendment_type = 'RESTATEMENT' OR (form = '13F-HR/A' AND amendment_type IS NULL)
       QUALIFY ROW_NUMBER() OVER (PARTITION BY cik, period ORDER BY filed DESC, accession DESC) = 1),
-- a NEW HOLDINGS amendment that re-files the whole table (>= 90% of the base's lines; 1,398 of 4,722 such amendments,
-- e.g. FMR 2023-Q1) replaces the base instead of adding to it - added on top it double-counted ~$38T across quarters
full_refile AS (SELECT a.* FROM f a JOIN b0 USING (cik, period)
                WHERE a.amendment_type = 'NEW HOLDINGS' AND a.filed >= b0.filed AND b0.holdings_lines > 0
                  AND a.holdings_lines >= 0.9 * b0.holdings_lines),
base AS (SELECT * FROM (SELECT * FROM b0 UNION ALL SELECT * FROM full_refile)
         WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY cik, period ORDER BY filed DESC, accession DESC) = 1)
SELECT b.cik, b.period, b.manager, b.accession, b.filed, b.form, b.report_type, 'base' AS role, b.entry_total, b.value_total_usd,
       b.holdings_value_usd, b.holdings_lines
FROM base b
UNION ALL
SELECT a.cik, a.period, b.manager, a.accession, a.filed, a.form, a.report_type, 'added', a.entry_total, a.value_total_usd,
       a.holdings_value_usd, a.holdings_lines
FROM f a JOIN base b ON a.cik = b.cik AND a.period = b.period
WHERE a.amendment_type = 'NEW HOLDINGS' AND a.filed >= b.filed AND a.accession != b.accession
  AND NOT (b.holdings_lines > 0 AND a.holdings_lines >= 0.9 * b.holdings_lines)""")
view("rrp.form13f_holdings", """-- every 13F holding line per manager per quarter (period = quarter end the report covers), amendments resolved through
-- rrp.form13f_reports, 2013 Q3 on (data sets) plus filings since Aug 24 2026 parsed from EDGAR daily. value_usd in
-- dollars (pre-2023 filings reported thousands; converted). shares_type SH = shares, PRN = principal amount (bonds);
-- put_call set = an option position (value = the underlying's value). 13F covers US-listed long positions only (no
-- shorts, no cash). Filter on cusip and / or period: the table is clustered on cusip and partitioned by period.
SELECT h.period, h.cik, r.manager, h.accession, h.filed, r.role, h.line, h.issuer, h.title_of_class, h.cusip, h.figi,
       h.value_usd, h.shares, h.shares_type, h.put_call, h.discretion, h.other_manager, h.vote_sole, h.vote_shared, h.vote_none
FROM `P.sec.form13f_holdings` h JOIN `P.rrp.form13f_reports` r ON r.accession = h.accession""")
view("rrp.form13f_managers", """-- one row per 13F manager per quarter: name, filing dates, total value (US$) and number of holding lines.
-- value_usd = the sum of the manager's own holding lines (sec.form13f_totals), falling back to the cover total when a
-- filing has no lines; cover_value_usd = the cover totals as typed (a few are typos). value_change_pct = vs the
-- manager's previous reported quarter. Cheap: no holdings scan.
WITH m AS (
  SELECT cik, period, ANY_VALUE(manager) AS manager, MIN(filed) AS first_filed, MAX(filed) AS last_filed,
         SUM(COALESCE(holdings_value_usd, value_total_usd)) AS value_usd, SUM(value_total_usd) AS cover_value_usd,
         SUM(entry_total) AS holdings, LOGICAL_OR(report_type LIKE '%COMBINATION%') AS combination,
         COUNTIF(role = 'added') AS amendments_added
  FROM `P.rrp.form13f_reports` GROUP BY cik, period)
SELECT *, ROUND(100 * SAFE_DIVIDE(value_usd - LAG(value_usd) OVER w, LAG(value_usd) OVER w), 2) AS value_change_pct
FROM m WINDOW w AS (PARTITION BY cik ORDER BY period)""")
view("rrp.form13f_by_security", """-- one row per security (CUSIP) per quarter across every 13F manager: holders, shares and value in common / long
-- positions (put_call NULL, shares_type SH), plus call and put option value, with the change vs the prior quarter.
-- Several managers can report the same shares (a parent and its sub-adviser, other_manager), so totals can overstate;
-- compare a security with its own history. Heavy: scans the holdings for the quarters asked; filter on period or cusip.
WITH s AS (
  SELECT period, cusip, ANY_VALUE(issuer) AS issuer, ANY_VALUE(title_of_class) AS title_of_class,
         COUNT(DISTINCT IF(put_call IS NULL, cik, NULL)) AS holders,
         SUM(IF(put_call IS NULL AND shares_type = 'SH', shares, 0)) AS shares,
         SUM(IF(put_call IS NULL, value_usd, 0)) AS value_usd,
         SUM(IF(UPPER(put_call) = 'CALL', value_usd, 0)) AS call_value_usd,
         SUM(IF(UPPER(put_call) = 'PUT', value_usd, 0)) AS put_value_usd
  FROM `P.rrp.form13f_holdings` GROUP BY period, cusip)
SELECT *, holders - LAG(holders) OVER w AS holders_change, shares - LAG(shares) OVER w AS shares_change,
       ROUND(100 * SAFE_DIVIDE(shares - LAG(shares) OVER w, LAG(shares) OVER w), 2) AS shares_change_pct
FROM s WINDOW w AS (PARTITION BY cusip ORDER BY period)""")

# ------------------------------------------------------------------ form144 / form13g / cusip map (added 2026-10-24, b167)
view("rrp.form144", """-- every Form 144 / 144/A (notice of proposed sale of restricted or control stock by an insider or affiliate, filed
-- on or before the sale), 2025 on, one row per security line. One filing can list several lines. ticker = the issuer's
-- ticker on the filing date (rrp.entity_tickers). Amounts in US$ as the filer typed them: implied_price =
-- market_value / units_to_sell, and value_suspect marks lines whose implied price is over $10,000 a share (a typo in
-- the filing, e.g. cents typed as dollars); leave those out of sums. pct_of_outstanding = units_to_sell /
-- units_outstanding x 100. under_10b5_1 = the seller named a Rule 10b5-1 trading plan (plan_adoption_date). A 144
-- is an intention, not a trade: the Form 4 that follows (rrp.insider_*) reports the actual sale.
WITH t AS (SELECT cik, ticker, valid_from, valid_to, form4_filings FROM `P.rrp.entity_tickers`)
SELECT f.accession, f.filed, f.form, f.form = '144/A' AS is_amendment, f.line, f.issuer_cik, f.issuer_name, t.ticker,
       f.seller, f.relationship, f.security_class, f.broker, f.units_to_sell, f.market_value,
       ROUND(SAFE_DIVIDE(f.market_value, f.units_to_sell), 4) AS implied_price,
       IFNULL(SAFE_DIVIDE(f.market_value, f.units_to_sell) > 10000 AND f.issuer_cik != '1067983', FALSE) AS value_suspect,
       f.units_outstanding, ROUND(100 * SAFE_DIVIDE(f.units_to_sell, f.units_outstanding), 4) AS pct_of_outstanding,
       f.approx_sale_date, f.exchange, f.acquired_date, f.acquisition_nature, f.sold_past_3m_units, f.sold_past_3m_value,
       f.plan_adoption_date, f.plan_adoption_date IS NOT NULL AS under_10b5_1, f.notice_date,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', f.issuer_cik, '/', f.accession, '/') AS url
FROM `P.sec.form144` f
LEFT JOIN t ON t.cik = f.issuer_cik AND f.filed BETWEEN t.valid_from AND IFNULL(t.valid_to, DATE '9999-12-31')
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.accession, f.line ORDER BY t.form4_filings DESC, f.ingested_at DESC) = 1""")
view("rrp.form13g", """-- every Schedule 13G / 13G/A (passive owner of more than 5% of a listed class), structured filings Dec 2024 on, loaded
-- from 2025, one row per filing. filer = the lead reporting person (highest pct when several file jointly; all names in
-- persons). rule: 13d-1(b) = institution (adviser, bank, broker, insurer: filer_type IA / HC / BD / BK / IC / FI),
-- 13d-1(c) = other passive investor over 5%, 13d-1(d) = held before the IPO / registration. pct = percent of the
-- class; shares = aggregate amount owned. cusip = 9-character CUSIP cleaned from what the filer typed (ISINs cut
-- down, check digit kept); cusip8 = the first 8 (issue) characters, the safe key to join rrp.form13f_* on.
-- prev_pct / pct_change = this filer's previous 13G on the same issuer; exited = an amendment at or under 5%
-- (the filer no longer has to report). ticker = the issuer's ticker on the filing date.
WITH g AS (
  SELECT *, CASE WHEN REGEXP_CONTAINS(c, r'^[A-Z]{2}[0-9A-Z]{9}[0-9]$') THEN SUBSTR(c, 3, 9)
                 WHEN LENGTH(c) >= 9 THEN SUBSTR(c, 1, 9) ELSE c END AS cusip9
  FROM (SELECT *, UPPER(REGEXP_REPLACE(IFNULL(cusip, ''), r'[^0-9A-Za-z]', '')) AS c FROM `P.sec.form13g`)
  WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY accession ORDER BY ingested_at DESC) = 1),
t AS (SELECT cik, ticker, valid_from, valid_to, form4_filings FROM `P.rrp.entity_tickers`),
j AS (
  SELECT g.*, t.ticker FROM g
  LEFT JOIN t ON t.cik = g.issuer_cik AND g.filed BETWEEN t.valid_from AND IFNULL(t.valid_to, DATE '9999-12-31')
  WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY g.accession ORDER BY t.form4_filings DESC) = 1)
SELECT accession, filed, form, form LIKE '%/A' AS is_amendment, amendment_no, issuer_cik, issuer_name, ticker,
       NULLIF(cusip9, '') AS cusip, NULLIF(SUBSTR(cusip9, 1, 8), '') AS cusip8, cusip AS cusip_as_filed, security_class,
       event_date, filer, filer_type, rule, pct, shares, sole_vote, shared_vote, sole_dispositive, shared_dispositive,
       persons, LAG(pct) OVER w AS prev_pct, ROUND(pct - LAG(pct) OVER w, 2) AS pct_change,
       form LIKE '%/A' AND pct <= 5 AS exited,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', issuer_cik, '/', accession, '/') AS url
FROM j WINDOW w AS (PARTITION BY issuer_cik, filer ORDER BY filed, accession)""")
view("rrp.cusip_map", """-- CUSIP -> company (SEC CIK) map: one row per 8-character CUSIP (cusip8, the issue without its check digit) with
-- the issuer's CIK, name and ticker. Use it to put a CIK / ticker on rrp.form13f_* rows: JOIN ON cusip8 = SUBSTR(cusip, 1, 8).
-- source '13g': from 13G filings, which name both the CUSIP and the issuer (the CIK most filings gave it; filings and
--   agreement = how many filings name it and the share that agree). Issuers with a 13G filer over 5% since 2025.
-- source 'figi' (b190, 2026-11-07): CUSIPs the 13G map can't place, looked up on OpenFIGI (sec.cusip_figi): an equity
--   with a US listing whose ticker is a current SEC ticker (rrp.cik_tickers) and whose name starts with the same word
--   as the SEC name (a reused ticker would fail that check); warrants, rights and SPAC units left out. filings = 0,
--   agreement = 1. 13G rows win where both exist.
WITH g AS (
  SELECT cusip8, ARRAY_AGG(STRUCT(issuer_cik, issuer_name, ticker, cusip) ORDER BY n DESC, last_filed DESC LIMIT 1)[OFFSET(0)].*,
         SUM(n) AS filings, MAX(n) / SUM(n) AS agreement, MIN(first_filed) AS first_filed, MAX(last_filed) AS last_filed
  FROM (SELECT cusip8, issuer_cik, ARRAY_AGG(issuer_name IGNORE NULLS ORDER BY filed DESC LIMIT 1)[SAFE_OFFSET(0)] AS issuer_name,
               ARRAY_AGG(ticker IGNORE NULLS ORDER BY filed DESC LIMIT 1)[SAFE_OFFSET(0)] AS ticker,
               ARRAY_AGG(cusip IGNORE NULLS ORDER BY filed DESC LIMIT 1)[SAFE_OFFSET(0)] AS cusip,
               COUNT(*) AS n, MIN(filed) AS first_filed, MAX(filed) AS last_filed
        FROM `P.rrp.form13g` WHERE cusip8 IS NOT NULL AND LENGTH(cusip8) = 8 GROUP BY cusip8, issuer_cik)
  GROUP BY cusip8),
f AS (
  SELECT x.cusip8, t.cik AS issuer_cik, t.name AS issuer_name, t.ticker, x.cusip
  FROM (SELECT * FROM `P.sec.cusip_figi` QUALIFY ROW_NUMBER() OVER (PARTITION BY cusip8 ORDER BY looked_up_at DESC) = 1) x
  JOIN `P.rrp.cik_tickers` t ON t.is_current AND t.ticker = REPLACE(REPLACE(x.ticker, '/', '-'), '.', '-')
  WHERE x.found AND x.market_sector = 'Equity'
    -- the stock itself only: warrants, rights and SPAC units have their own tickers under the same CIK and would add
    -- their holders to the company's (a 5-letter ticker ending in U is a SPAC unit; other units are LP common units)
    AND x.security_type NOT IN ('Equity WRT', 'Right')
    AND NOT (x.security_type = 'Unit' AND LENGTH(x.ticker) = 5 AND ENDS_WITH(x.ticker, 'U'))
    AND x.exch_code IN ('US', 'UN', 'UQ', 'UW', 'UA', 'UR', 'UP', 'UF', 'UV', 'PQ')
    AND REGEXP_EXTRACT(UPPER(x.name), r'[A-Z0-9]+') = REGEXP_EXTRACT(UPPER(t.name), r'[A-Z0-9]+')
  QUALIFY COUNT(*) OVER (PARTITION BY x.cusip8) = 1)
SELECT g.*, '13g' AS source FROM g
UNION ALL
SELECT f.cusip8, f.issuer_cik, f.issuer_name, f.ticker, f.cusip, 0, 1.0, CAST(NULL AS DATE), CAST(NULL AS DATE), 'figi'
FROM f WHERE f.cusip8 NOT IN (SELECT cusip8 FROM g)""")

# ------------------------------------------------------------------ fund -> ETF proxy map (added 2026-10-26)
view("rrp.form13f_position_changes", """-- what each 13F manager bought and sold in its newest quarter (b212, 2026-10-26): for the latest quarter end and the one
-- before it, only managers that have already filed the latest quarter (managers that have not filed yet would otherwise
-- show every position as an exit). One row per manager x security (common / long share positions, put_call NULL and
-- shares_type SH, summed over the filing's lines). change: new (no position last quarter), exit (none now), added, cut,
-- unchanged. shares_change_pct vs last quarter; value_usd now; pct_of_portfolio = this position / the manager's 13F total;
-- est_trade_usd = shares change x this quarter's value per share (a rough dollar size for adds and cuts; exits use last
-- quarter's value). Kept: positions worth $25M+ now or last quarter. ticker from rrp.cusip_map (13G-based; smaller
-- issuers have none). suspect = the trade is bigger than the manager's whole portfolio in either quarter, or the manager's
-- total or the position's value per share moved 10x between quarters (a share count or units typed wrong in one filing):
-- leave those out. Filter on manager / cik or ticker; the newest quarter fills in through
-- the 45-day filing window. ~580 MB a read.
WITH q AS (SELECT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY) AS cur),
qq AS (SELECT cur, DATE_SUB(DATE_SUB(DATE_ADD(cur, INTERVAL 1 DAY), INTERVAL 1 QUARTER), INTERVAL 1 DAY) AS prev FROM q),
pos AS (
  SELECT period, cik, ANY_VALUE(manager) AS manager, cusip, ANY_VALUE(issuer) AS issuer, ANY_VALUE(title_of_class) AS title_of_class,
         SUM(shares) AS shares, SUM(value_usd) AS value_usd
  FROM `P.rrp.form13f_holdings`
  WHERE period IN (DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY),   -- written out (not read from qq) so
                   DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER), INTERVAL 1 DAY))   -- BigQuery prunes partitions
    AND put_call IS NULL AND shares_type = 'SH'
  GROUP BY period, cik, cusip),
filed_cur AS (SELECT DISTINCT cik FROM pos WHERE period = (SELECT cur FROM qq)),
tot AS (SELECT cik, period, value_usd AS manager_value_usd FROM `P.rrp.form13f_managers`
        WHERE period IN ((SELECT cur FROM qq), (SELECT prev FROM qq))),
a AS (SELECT * FROM pos WHERE period = (SELECT cur FROM qq)),
b AS (SELECT * FROM pos WHERE period = (SELECT prev FROM qq) AND cik IN (SELECT cik FROM filed_cur)),
j AS (
  SELECT COALESCE(a.cik, b.cik) AS cik, COALESCE(a.manager, b.manager) AS manager, COALESCE(a.cusip, b.cusip) AS cusip,
         COALESCE(a.issuer, b.issuer) AS issuer, COALESCE(a.title_of_class, b.title_of_class) AS title_of_class,
         IFNULL(a.shares, 0) AS shares, IFNULL(b.shares, 0) AS prev_shares, IFNULL(a.value_usd, 0) AS value_usd,
         IFNULL(b.value_usd, 0) AS prev_value_usd
  FROM a FULL JOIN b ON a.cik = b.cik AND a.cusip = b.cusip)
SELECT qq.cur AS period, qq.prev AS prev_period, j.cik, j.manager, j.cusip, m.ticker, j.issuer, j.title_of_class,
       CASE WHEN j.prev_shares = 0 THEN 'new' WHEN j.shares = 0 THEN 'exit' WHEN j.shares > j.prev_shares THEN 'added'
            WHEN j.shares < j.prev_shares THEN 'cut' ELSE 'unchanged' END AS change,
       j.shares, j.prev_shares, j.shares - j.prev_shares AS shares_change,
       ROUND(100 * SAFE_DIVIDE(j.shares - j.prev_shares, j.prev_shares), 1) AS shares_change_pct,
       j.value_usd, j.prev_value_usd,
       ROUND(IF(j.shares = 0, -j.prev_value_usd, (j.shares - j.prev_shares) * SAFE_DIVIDE(j.value_usd, j.shares))) AS est_trade_usd,
       ROUND(100 * SAFE_DIVIDE(j.value_usd, t.manager_value_usd), 2) AS pct_of_portfolio, t.manager_value_usd,
       tp.manager_value_usd AS prev_manager_value_usd,
       -- a trade bigger than the whole portfolio in either quarter: a share count or value typed wrong in one of the filings
       IFNULL(ABS(IF(j.shares = 0, j.prev_value_usd, (j.shares - j.prev_shares) * SAFE_DIVIDE(j.value_usd, j.shares)))
         > GREATEST(IFNULL(t.manager_value_usd, 0), IFNULL(tp.manager_value_usd, 0))
       -- or the manager's whole 13F moved 10x or more between quarters, or the position's value per share did (units typed wrong)
       OR SAFE_DIVIDE(GREATEST(t.manager_value_usd, tp.manager_value_usd), LEAST(t.manager_value_usd, tp.manager_value_usd)) >= 10
       OR SAFE_DIVIDE(GREATEST(SAFE_DIVIDE(j.value_usd, j.shares), SAFE_DIVIDE(j.prev_value_usd, j.prev_shares)),
                      LEAST(SAFE_DIVIDE(j.value_usd, j.shares), SAFE_DIVIDE(j.prev_value_usd, j.prev_shares))) >= 10, FALSE) AS suspect
FROM j CROSS JOIN qq
LEFT JOIN tot t ON t.cik = j.cik AND t.period = qq.cur
LEFT JOIN tot tp ON tp.cik = j.cik AND tp.period = qq.prev
LEFT JOIN `P.rrp.cusip_map` m ON m.cusip8 = SUBSTR(j.cusip, 1, 8)
WHERE GREATEST(j.value_usd, j.prev_value_usd) >= 25e6""")
view("rrp.fund_etf_map", """-- Fidelity fund -> the ETF whose daily moves it tracks most closely (the proxy_etf), so ETF data (flows, short interest,
-- volume) can stand in for a mutual fund that publishes none. Pairs set by hand on 2026-10-26 and checked against every
-- candidate ETF: the proxy is the ETF with the highest daily-return correlation over the fund's last 252 trading days.
-- corr_1y / beta_1y are recomputed on every read over the trailing 252 days (beta = the fund's move per 1% ETF move;
-- FSAGX ~1x GDX, ~1.4x GLD). fit: strong >= 0.90, moderate 0.80-0.90, weak < 0.80 (proxy flows say little about the
-- fund). secondary_etf = the next-best or larger-flow alternative. Not mapped: FERLX (cash-like) and the non-Fidelity funds.
WITH m AS (SELECT * FROM UNNEST([
    STRUCT('FSUTX' AS fund, 'Select Utilities' AS fund_name, 'sector' AS kind, 'XLU' AS proxy_etf, CAST(NULL AS STRING) AS secondary_etf),
    ('FSENX', 'Select Energy', 'sector', 'XLE', 'XOP'),
    ('FNARX', 'Select Natural Resources', 'sector', 'IGE', 'GUNR'),
    ('FDFAX', 'Select Consumer Staples', 'sector', 'XLP', NULL),
    ('FSCPX', 'Select Consumer Discretionary', 'sector', 'XLY', 'IYC'),
    ('FSRPX', 'Select Retailing', 'sector', 'IYC', 'XRT'),
    ('FSAVX', 'Select Automotive', 'sector', 'XLY', 'IDRV'),
    ('FSHOX', 'Select Construction & Housing', 'sector', 'XHB', 'ITB'),
    ('FSPHX', 'Select Health Care', 'sector', 'IBB', 'XLV'),
    ('FBIOX', 'Select Biotechnology', 'sector', 'XBI', 'IBB'),
    ('FPHAX', 'Select Pharmaceuticals', 'sector', 'IHE', 'XLV'),
    ('FSHCX', 'Select Health Care Services', 'sector', 'IHF', NULL),
    ('FSMEX', 'Select Medical Technology & Devices', 'sector', 'IHI', 'XLV'),
    ('FIDSX', 'Select Financial Services', 'sector', 'XLF', NULL),
    ('FSRBX', 'Select Banking', 'sector', 'KBE', 'KRE'),
    ('FSLBX', 'Select Brokerage & Investment Mgmt', 'sector', 'KCE', 'IAI'),
    ('FSPCX', 'Select Insurance', 'sector', 'KIE', NULL),
    ('FSPTX', 'Select Technology', 'sector', 'XLK', 'QQQ'),
    ('FSELX', 'Select Semiconductors', 'sector', 'SMH', 'SOXX'),
    ('FSCSX', 'Select Software & IT Services', 'sector', 'IGV', NULL),
    ('FBMPX', 'Select Communication Services', 'sector', 'VOX', 'XLC'),
    ('FSTCX', 'Select Telecommunications', 'sector', 'IYZ', 'XTL'),
    ('FWRLX', 'Select Wireless', 'sector', 'NXTG', 'XTL'),
    ('FSDAX', 'Select Defense & Aerospace', 'sector', 'ITA', 'PPA'),
    ('FSRFX', 'Select Transportation', 'sector', 'IYT', 'XTN'),
    ('FSLEX', 'Select Environment & Alternative Energy', 'sector', 'VIS', 'QCLN'),
    ('FSDPX', 'Select Materials', 'sector', 'VAW', 'XLB'),
    ('FSCHX', 'Select Chemicals', 'sector', 'XLB', 'VAW'),
    ('FSAGX', 'Select Gold', 'sector', 'GDX', 'GLD'),
    ('FXAIX', '500 Index', 'broad', 'SPY', NULL),
    ('FBGRX', 'Blue Chip Growth', 'broad', 'IWF', 'QQQ'),
    ('FSMDX', 'Mid Cap Index', 'broad', 'IJH', NULL),
    ('FSSNX', 'Small Cap Index', 'broad', 'IWM', NULL),
    ('FSPSX', 'International Index', 'broad', 'EFA', NULL),
    ('FPADX', 'Emerging Markets Index', 'broad', 'IEMG', 'EEM'),
    ('FXNAX', 'U.S. Bond Index', 'bond', 'AGG', NULL),
    ('FIPDX', 'Inflation-Protected Bond Index', 'bond', 'TIP', NULL),
    ('FUMBX', 'Short-Term Treasury Bond Index', 'bond', 'SHY', 'VGSH')])),
px AS (SELECT symbol, date, SAFE_DIVIDE(adj_close, LAG(adj_close) OVER (PARTITION BY symbol ORDER BY date)) - 1 AS r
       FROM `P.markets.equity_daily`
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
         AND symbol IN (SELECT fund FROM m UNION ALL SELECT proxy_etf FROM m UNION ALL SELECT secondary_etf FROM m WHERE secondary_etf IS NOT NULL)),
pr AS (SELECT m.fund, m.proxy_etf, f.date, f.r AS rf, x.r AS rx FROM m
       JOIN px f ON f.symbol = m.fund JOIN px x ON x.symbol = m.proxy_etf AND x.date = f.date
       WHERE f.r IS NOT NULL AND x.r IS NOT NULL
       QUALIFY ROW_NUMBER() OVER (PARTITION BY m.fund ORDER BY f.date DESC) <= 252),
st AS (SELECT fund, ROUND(CORR(rf, rx), 3) AS corr_1y, ROUND(COVAR_SAMP(rf, rx) / VAR_SAMP(rx), 2) AS beta_1y, COUNT(*) AS days,
              MAX(date) AS through FROM pr GROUP BY fund)
SELECT m.fund, m.fund_name, m.kind, m.proxy_etf, m.secondary_etf, st.corr_1y, st.beta_1y,
       CASE WHEN st.corr_1y >= 0.9 THEN 'strong' WHEN st.corr_1y >= 0.8 THEN 'moderate' ELSE 'weak' END AS fit,
       st.days, st.through
FROM m LEFT JOIN st USING (fund)""")

view("rrp.etf_share_flows", """-- daily creations / redemptions for every ETF in markets.etf_shares (SPDR, GLD and VanEck GDX / GDXJ / SMH history from
-- 2005-2006 or inception, every iShares ETF from 2026-10-23): flow_usd = change in shares outstanding x that day's NAV (+ = money in, - = out).
-- prev_date = the fund's previous loaded day; gap_days > 4 means a hole (a missed file), so treat that flow as a
-- multi-day total. flow_pct = flow / the previous day's total net assets x 100. flow_5d / flow_20d = sums over the
-- last 5 / 20 loaded days. split_factor != 1 = a share split that day (NAV moved by a clean multiple overnight, e.g.
-- XES 1-for-10 on 2020-03-30): the previous share count is scaled by it, so a split is not read as a flow. Issuers
-- report shares outstanding a day after the creations settle, so a flow can land one trading day after the trade
-- (KRE's +8.1M shares show on 2026-10-16). Early days of a new fund (50k-share base) give flow_pct in the 100s - real.
-- Mutual funds have no daily flows: rrp.fund_flows maps them onto these ETFs. (The crypto spot-ETF flows from Farside
-- are rrp.etf_flows_daily, a different source.)
WITH s AS (SELECT * FROM `P.markets.etf_shares` WHERE nav > 0 AND shares_out > 0
           QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY ingested_at DESC) = 1),
c AS (SELECT *, LAG(date) OVER w AS prev_date, LAG(shares_out) OVER w AS prev_shares, SAFE_DIVIDE(LAG(nav) OVER w, nav) AS r,
             LAG(total_net_assets) OVER w AS prev_assets
      FROM s WINDOW w AS (PARTITION BY symbol ORDER BY date)),
k AS (SELECT *, CASE WHEN r >= 1.8 THEN ROUND(r) WHEN r <= 0.55 THEN 1 / ROUND(1 / r) ELSE 1 END AS split_factor FROM c),
v AS (SELECT *, shares_out - prev_shares * split_factor AS shares_change,
             ROUND((shares_out - prev_shares * split_factor) * nav) AS flow_usd FROM k WHERE prev_date IS NOT NULL)
SELECT date, symbol, issuer, fund_name, nav, shares_out, total_net_assets, prev_date, DATE_DIFF(date, prev_date, DAY) AS gap_days,
       split_factor, shares_change, flow_usd, ROUND(100 * SAFE_DIVIDE(flow_usd, prev_assets), 3) AS flow_pct,
       SUM(flow_usd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS flow_5d,
       SUM(flow_usd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS flow_20d,
       ROUND(100 * SAFE_DIVIDE(SUM(flow_usd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                               LAG(total_net_assets, 20) OVER (PARTITION BY symbol ORDER BY date)), 3) AS flow_20d_pct
FROM v""")
view("rrp.fund_flows", """-- daily ETF flows standing in for each mapped Fidelity fund (rrp.fund_etf_map): etf = the proxy ETF when it has flow
-- data, else the secondary (VOX, VIS, VAW, NXTG, QQQ have no free shares-outstanding file, so FBMPX -> XLC,
-- FSDPX -> XLB, FSLEX -> QCLN when loaded; GDX, SMH and GLD come from VanEck / SPDR Gold). fit / corr_1y say how closely the fund tracks that ETF:
-- read 'weak' rows as context only. Flows are the ETF's, in US$, not the mutual fund's own.
WITH m AS (SELECT * FROM `P.rrp.fund_etf_map`),
have AS (SELECT DISTINCT symbol FROM `P.markets.etf_shares`),
pick AS (SELECT m.*, CASE WHEN m.proxy_etf IN (SELECT symbol FROM have) THEN m.proxy_etf
                          WHEN m.secondary_etf IN (SELECT symbol FROM have) THEN m.secondary_etf END AS etf
         FROM m)
SELECT p.fund, p.fund_name, p.kind, p.etf, p.etf = p.proxy_etf AS is_proxy, p.fit, p.corr_1y, p.beta_1y,
       f.date, f.flow_usd, f.flow_pct, f.flow_5d, f.flow_20d, f.flow_20d_pct, f.total_net_assets AS etf_assets, f.gap_days
FROM pick p JOIN `P.rrp.etf_share_flows` f ON f.symbol = p.etf""")

# ------------------------------------------------------------------ Form ADV advisers (added 2026-10-26, b167)
view("rrp.adv_firm_months", """-- every SEC investment adviser, one row per firm (crd = its FINRA CRD number, the stable id) per monthly SEC snapshot,
-- Feb 2019 on (Oct / Nov 2025 and Jan 2019 missing: SEC posted no file). kind: registered (an RIA filing the full Form
-- ADV: regulatory AUM, accounts, employees) or exempt (an exempt reporting adviser - private-fund or VC adviser - with no
-- AUM in this file). aum_total = regulatory assets under management in US$ (sub-advised money is counted by every
-- adviser on it, so totals across firms overstate). aum_change_pct = vs the firm's previous snapshot; prev_kind shows a
-- move from exempt to registered. cik is set for ~25% of registered firms (those that also file with EDGAR).
WITH m AS (SELECT * FROM `P.sec.adv_firms` WHERE crd IS NOT NULL AND crd != ''
           QUALIFY ROW_NUMBER() OVER (PARTITION BY crd, as_of ORDER BY IF(kind = 'registered', 0, 1), ingested_at DESC) = 1)
SELECT as_of, kind, LAG(kind) OVER w AS prev_kind, crd, sec_number, cik, firm_name, legal_name, city, state, country,
       adv_filing_date, website, employees, aum_total, aum_discretionary, aum_nondiscretionary, accounts_total,
       LAG(aum_total) OVER w AS prev_aum, ROUND(100 * SAFE_DIVIDE(aum_total - LAG(aum_total) OVER w, LAG(aum_total) OVER w), 2) AS aum_change_pct
FROM m WINDOW w AS (PARTITION BY crd ORDER BY as_of)""")
view("rrp.adv_firms", """-- every SEC investment adviser in the newest monthly snapshot, one row per firm, with its history: first_listed = the first
-- snapshot it appears in (2019-02-01 = it was already there when the data starts), first_registered = first month as a
-- registered adviser (an exempt adviser that later registered shows both), months_listed, aum_1y_ago and
-- aum_1y_change_pct (vs the snapshot nearest 12 months earlier). last_13f_period / last_13f_value_usd = the firm's
-- newest 13F report (rrp.form13f_managers), joined on SEC CIK, so only for firms whose ADV gives a CIK. Firms that left
-- (deregistered, merged) are not here: see rrp.adv_firm_months. Reads the whole history (~420 MB).
WITH m AS (SELECT * FROM `P.rrp.adv_firm_months`),
lat AS (SELECT MAX(as_of) AS d, MIN(as_of) AS d0 FROM m),
hist AS (SELECT crd, MIN(as_of) AS first_listed, MIN(IF(kind = 'registered', as_of, NULL)) AS first_registered,
                COUNT(*) AS months_listed FROM m GROUP BY crd),
yago AS (SELECT crd, aum_total AS aum_1y_ago FROM m, lat
         WHERE as_of BETWEEN DATE_SUB(lat.d, INTERVAL 14 MONTH) AND DATE_SUB(lat.d, INTERVAL 10 MONTH)
         QUALIFY ROW_NUMBER() OVER (PARTITION BY crd ORDER BY ABS(DATE_DIFF(as_of, DATE_SUB(lat.d, INTERVAL 12 MONTH), DAY))) = 1),
f13 AS (SELECT cik, ARRAY_AGG(STRUCT(period, value_usd) ORDER BY period DESC LIMIT 1)[OFFSET(0)] AS last
        FROM `P.rrp.form13f_managers` GROUP BY cik)
SELECT m.as_of, m.kind, m.crd, m.sec_number, m.cik, m.firm_name, m.legal_name, m.city, m.state, m.country, m.adv_filing_date,
       m.website, m.employees, m.aum_total, m.aum_discretionary, m.accounts_total, y.aum_1y_ago,
       ROUND(100 * SAFE_DIVIDE(m.aum_total - y.aum_1y_ago, y.aum_1y_ago), 2) AS aum_1y_change_pct,
       h.first_listed, h.first_registered, h.months_listed, h.first_listed = lat.d0 AS listed_at_start,
       f.last.period AS last_13f_period, f.last.value_usd AS last_13f_value_usd
FROM m CROSS JOIN lat JOIN hist h USING (crd) LEFT JOIN yago y USING (crd) LEFT JOIN f13 f ON f.cik = m.cik
WHERE m.as_of = lat.d""")

# ------------------------------------------------------------------ EIA weekly + degree days (added 2026-10-26)
view("rrp.eia_weekly", """-- EIA weekly series, one row per series per week (week_ending = the report's period): natural gas storage (lower 48
-- and regions, Bcf; out Thursdays) and petroleum status (crude, gasoline, distillate, Cushing and SPR stocks, production,
-- imports, exports, refinery runs; out Wednesdays), 2000 / 2010 on. change = vs the previous week; change_1y = vs the
-- same ISO week a year earlier; avg_5y = the average for the same ISO week over the 5 prior years, and vs_5y_pct = how
-- far this week sits from it (the comparison the weekly storage report leads with). Units per series in units.
WITH s AS (SELECT *, EXTRACT(ISOWEEK FROM period) AS wk, EXTRACT(ISOYEAR FROM period) AS yr FROM `P.gov.eia_weekly`),
f AS (SELECT a.series, a.period, AVG(b.value) AS avg_5y, COUNT(b.value) AS years_in_avg
      FROM s a JOIN s b ON a.series = b.series AND b.wk = a.wk AND b.yr BETWEEN a.yr - 5 AND a.yr - 1 GROUP BY 1, 2),
y AS (SELECT a.series, a.period, b.value AS value_1y FROM s a JOIN s b ON a.series = b.series AND b.wk = a.wk AND b.yr = a.yr - 1)
SELECT s.period AS week_ending, s.route, s.series, s.series_name, s.area, s.product, s.value, s.units,
       s.value - LAG(s.value) OVER (PARTITION BY s.series ORDER BY s.period) AS change,
       s.value - y.value_1y AS change_1y, ROUND(f.avg_5y, 1) AS avg_5y, f.years_in_avg,
       ROUND(100 * SAFE_DIVIDE(s.value - f.avg_5y, ABS(f.avg_5y)), 2) AS vs_5y_pct
FROM s LEFT JOIN f USING (series, period) LEFT JOIN y USING (series, period)""")
view("rrp.degree_days", """-- NOAA CPC daily heating (HDD) and cooling (CDD) degree days by census division and contiguous US, 2000 on (~2-day
-- lag), weighted by population, by homes heating with natural gas ('gas' heating = the gas-demand series traders watch)
-- or by electric heat. kind heating / cooling. sum_7d = the last 7 days; value_1y = the same calendar day a year earlier;
-- avg_10y = the same day's average over the 10 prior years; vs_10y = this day minus that average (+ = colder than
-- normal for heating, hotter for cooling). Filter on region = 'CONUS' for the national figure.
WITH s AS (SELECT * , EXTRACT(DAYOFYEAR FROM date) AS doy, EXTRACT(YEAR FROM date) AS yr FROM `P.gov.degree_days`),
n AS (SELECT a.date, a.region, a.kind, a.weights, AVG(b.value) AS avg_10y
      FROM s a JOIN s b ON a.region = b.region AND a.kind = b.kind AND a.weights = b.weights AND b.doy = a.doy
                       AND b.yr BETWEEN a.yr - 10 AND a.yr - 1
      GROUP BY 1, 2, 3, 4)
SELECT s.date, s.region, s.region_name, s.kind, s.weights, s.value,
       SUM(s.value) OVER (PARTITION BY s.region, s.kind, s.weights ORDER BY s.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_7d,
       LAG(s.value, 365) OVER (PARTITION BY s.region, s.kind, s.weights ORDER BY s.date) AS value_1y,
       ROUND(n.avg_10y, 1) AS avg_10y, ROUND(s.value - n.avg_10y, 1) AS vs_10y
FROM s LEFT JOIN n USING (date, region, kind, weights)""")

# ------------------------------------------------------------------ entity map (added 2026-10-23, point-in-time joins)
view("rrp.entity_tickers", """-- point-in-time ticker <-> company (SEC CIK) map: one row per company per ticker it traded under, with the dates the
-- ticker was seen on that company (Form 4 filings since 2006, which name the issuer's ticker, plus the SEC's daily
-- ticker list since Oct 2026). A ticker reused by a later company (1,000+ have been) gets one row per company:
-- valid_from / valid_to never overlap for a ticker, so to find who 'ABC' was on a date use
--   WHERE ticker = 'ABC' AND d BETWEEN valid_from AND IFNULL(valid_to, CURRENT_DATE()).
-- valid_from = the first sighting (an approximation of the listing date for names with few Form 4s); valid_to = the
-- earliest of: the day before the next company took the ticker, the day before this company's next ticker (FB -> META),
-- its delisting (Form 25 / 15), else the last sighting; NULL while the ticker is current. Form 4 tickers are
-- typed by filers: pairs seen on only one filing and not on the SEC list are left out. delisted_on = the first Form 25 / 15
-- the company filed from 30 days before the ticker was last seen on (sec.delistings).
WITH f4 AS (SELECT issuer_cik AS cik, UPPER(TRIM(ticker)) AS ticker, MIN(filed) AS first_seen, MAX(filed) AS last_seen,
                   COUNT(DISTINCT accession) AS n
            FROM `P.sec.form4` WHERE REGEXP_CONTAINS(UPPER(TRIM(ticker)), r'^[A-Z]{1,5}([.-][A-Z]{1,2})?$') GROUP BY 1, 2),
sn AS (SELECT cik, ticker, MIN(as_of) AS first_seen, MAX(as_of) AS last_seen, ANY_VALUE(name) AS name,
              MAX(as_of) = (SELECT MAX(as_of) FROM `P.sec.cik_tickers`) AS is_current
       FROM `P.sec.cik_tickers` WHERE ticker != '' GROUP BY 1, 2),
u AS (SELECT COALESCE(f.cik, s.cik) AS cik, COALESCE(f.ticker, s.ticker) AS ticker,
             LEAST(IFNULL(f.first_seen, DATE '9999-12-31'), IFNULL(s.first_seen, DATE '9999-12-31')) AS first_seen,
             GREATEST(IFNULL(f.last_seen, DATE '0001-01-01'), IFNULL(s.last_seen, DATE '0001-01-01')) AS last_seen,
             IFNULL(s.is_current, FALSE) AS is_current, s.name, IFNULL(f.n, 0) AS form4_filings,
             CONCAT(IF(f.cik IS NOT NULL, 'form4', ''), IF(f.cik IS NOT NULL AND s.cik IS NOT NULL, '+', ''),
                    IF(s.cik IS NOT NULL, 'sec_list', '')) AS sources
      FROM f4 f FULL OUTER JOIN sn s ON s.cik = f.cik AND s.ticker = f.ticker
      WHERE s.cik IS NOT NULL OR f.n >= 2),
e AS (SELECT *, LEAD(first_seen) OVER (PARTITION BY ticker ORDER BY first_seen, cik) AS next_first,   -- next company on the ticker
             LEAD(first_seen) OVER (PARTITION BY cik ORDER BY first_seen, ticker) AS next_own FROM u),  -- this company's next ticker
nm AS (SELECT issuer_cik AS cik, ARRAY_AGG(issuer ORDER BY filed DESC LIMIT 1)[OFFSET(0)] AS issuer FROM `P.sec.form4` GROUP BY 1),
dl AS (SELECT e.cik, e.ticker, MIN(d.filed) AS delisted_on FROM e JOIN `P.sec.delistings` d
         ON d.cik = e.cik AND d.filed >= DATE_SUB(e.last_seen, INTERVAL 30 DAY) GROUP BY 1, 2)
SELECT e.ticker, e.cik, COALESCE(e.name, nm.issuer) AS company, e.first_seen AS valid_from,
       CASE WHEN e.is_current AND e.next_first IS NULL THEN NULL
            ELSE LEAST(IFNULL(DATE_SUB(e.next_first, INTERVAL 1 DAY), DATE '9999-12-31'),
                       -- a ticker change (FB -> META) caps the old one; share classes seen side by side (GOOG / GOOGL) do not
                       IF(e.last_seen < e.next_own, DATE_SUB(e.next_own, INTERVAL 1 DAY), DATE '9999-12-31'),
                       COALESCE(dl.delisted_on, IF(e.next_first IS NULL AND NOT IFNULL(e.last_seen < e.next_own, FALSE), e.last_seen, NULL), DATE '9999-12-31'))
       END AS valid_to,
       e.is_current, e.last_seen, e.form4_filings, e.sources, dl.delisted_on
FROM e LEFT JOIN nm USING (cik) LEFT JOIN dl USING (cik, ticker)""")

# ------------------------------------------------------------------ equity_*: US stocks and ETFs
view("rrp.equity_daily", """-- daily bars per ticker (Yahoo chart API). adj_close is split/dividend adjusted: use it for returns
SELECT symbol, date, open, high, low, close, adj_close, volume, currency, exchange, instrument_type
FROM `P.markets.equity_daily`""")
view("rrp.equity_watchlist", r"""-- tickers the daily equities pass must cover, with why each one is on the list. priority orders first-time backfills
WITH core AS (
  SELECT t AS ticker, 'core' AS reason, 1 AS priority FROM UNNEST([
    'SPY','QQQ','IWM','DIA','TLT','HYG','LQD','GLD','XLE','XLF','XLK','XLU','ITA','KRE',
    'MSTR','COIN','MARA','RIOT','CLSK','HUT','CORZ','HOOD','IBIT','FBTC','ETHA','GLXY','CRCL','BMNR','SBET',
    'LMT','RTX','NOC','GD','HII','LHX','BA','LDOS','BAH','KTOS','SAIC','CACI','TPC','PSN','AVAV','KRMN']) t),
funds AS (
  SELECT t AS ticker, 'fund_universe' AS reason, 1 AS priority FROM UNNEST([
    'FXAIX','FSPSX','SWSSX','FSMDX','FSSNX','FPADX','FBGRX','MINDX',
    'FSUTX','FSPTX','FSDAX','FSENX','FSAGX','FIDSX','FSMEX','FSELX','FSCSX','FBIOX','FSPHX','FPHAX',
    'FSHCX','FSRPX','FSRFX','FSCHX','FSDPX','FSHOX','FSLBX','FSRBX','FSPCX','FSCPX','FDFAX','FSTCX',
    'FSAIX','FSAVX','FSLEX','FNARX','FCYIX','FWRLX','FSDCX','FBMPX','FSESX',
    'FXNAX','FUMBX','FIPDX']) t),
ins AS (
  SELECT ticker, 'insider_buy' AS reason, 2 AS priority FROM `P.rrp.insider_buys`
  WHERE tx_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 180 DAY)
  GROUP BY ticker HAVING SUM(value) >= 100000),
fh AS (
  SELECT DISTINCT REGEXP_EXTRACT(filer, r'\(([A-Z]{1,5}(?:-[A-Z]{1,2})?)[,)]') AS ticker, CONCAT('filing:', stream) AS reason,
         IF(stream IN ('merger_agreement', 'tender_third_party', 'tender_issuer', 'activist_13d'), 3, 4) AS priority
  FROM `P.rrp.sec_filings` WHERE file_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 90 DAY))
SELECT ticker, MIN(priority) AS priority, STRING_AGG(DISTINCT reason, ', ' ORDER BY reason) AS reasons
FROM (SELECT * FROM core UNION ALL SELECT * FROM funds UNION ALL SELECT * FROM ins UNION ALL SELECT * FROM fh)
WHERE ticker IS NOT NULL AND ticker NOT IN ('NONE', 'NA')
GROUP BY ticker""")
view("rrp.equity_coverage", r"""-- every ticker the daily equities pass prices: the watchlist plus every listed US stock/ETF in the latest Nasdaq
-- Trader directory, without test issues, preferreds ($) and warrants/rights/units. Read by the rrp-market job.
WITH u AS (
  SELECT symbol, exchange, is_etf FROM `P.markets.equity_universe`
  WHERE as_of = (SELECT MAX(as_of) FROM `P.markets.equity_universe`)
    AND NOT test_issue AND NOT REGEXP_CONTAINS(symbol, r'[$^=]')
    AND NOT REGEXP_CONTAINS(LOWER(name), r'\b(warrants?|rights?|units?)\b'))
SELECT ticker, MIN(priority) AS priority, STRING_AGG(DISTINCT reasons, ', ') AS reasons FROM (
  SELECT ticker, priority, reasons FROM `P.rrp.equity_watchlist`
  UNION ALL SELECT symbol, 5, CONCAT('listed:', exchange, IF(is_etf, ' ETF', '')) FROM u)
GROUP BY ticker""")

view("rrp.insider_buys_priced", """-- clean open-market buys with the market close on (or just before) the trade date. price_mismatch = the Form 4
-- price is more than 1.5x away from the close, which usually means a non-USD price or a unit error
WITH j AS (
  SELECT b.*, e.close AS mkt_close, e.date AS mkt_date
  FROM `P.rrp.insider_buys` b
  LEFT JOIN `P.markets.equity_daily` e
    ON e.symbol = b.ticker AND e.date <= b.tx_date AND e.date > DATE_SUB(b.tx_date, INTERVAL 7 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.accession, b.insider, b.tx_date, CAST(b.shares AS STRING), CAST(b.price AS STRING) ORDER BY e.date DESC) = 1)
SELECT *, ROUND(SAFE_DIVIDE(price, mkt_close), 3) AS px_ratio,
       mkt_close IS NOT NULL AND SAFE_DIVIDE(price, mkt_close) NOT BETWEEN 0.667 AND 1.5 AS price_mismatch
FROM j""")
view("rrp.insider_clusters", """-- 30-day insider buying clusters: 3+ distinct insiders, $250K+ in clean open-market buys, price-checked against the
-- market close. net_seller_flag = insiders sold more than they bought in the same 30 days
WITH b AS (
  SELECT * FROM `P.rrp.insider_buys_priced`
  WHERE tx_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY) AND NOT IFNULL(price_mismatch, FALSE)),
s AS (
  SELECT ticker, SUM(-value) AS sold_30d, COUNT(DISTINCT insider) AS sellers_30d
  FROM `P.rrp.insider_trades`
  WHERE code = 'S' AND form = '4' AND NOT value_suspect AND tx_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY)
  GROUP BY ticker),
last_px AS (
  SELECT symbol, close AS last_close, date AS last_close_date FROM `P.markets.equity_daily`
  WHERE date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1)
SELECT b.ticker, ANY_VALUE(b.issuer) AS issuer,
  COUNT(DISTINCT b.insider) AS buyers,
  ROUND(SUM(b.value)) AS bought_usd,
  ROUND(SUM(b.value) / COUNT(DISTINCT b.insider)) AS avg_per_buyer_usd,
  ROUND(SAFE_DIVIDE(SUM(b.value), SUM(b.shares)), 2) AS vwap,
  MIN(b.tx_date) AS first_tx, MAX(b.tx_date) AS last_tx, MAX(b.filed) AS last_filed,
  STRING_AGG(DISTINCT b.role, ' | ') AS roles,
  COUNT(DISTINCT IF(REGEXP_CONTAINS(LOWER(IFNULL(b.role, '')), r'ceo|chief executive|cfo|chief financial|president|chair'), b.insider, NULL)) AS top_exec_buyers,
  IFNULL(s.sold_30d, 0) AS sold_usd_30d, IFNULL(s.sellers_30d, 0) AS sellers_30d,
  IFNULL(s.sold_30d, 0) > SUM(b.value) AS net_seller_flag,
  ANY_VALUE(p.last_close) AS last_close, ANY_VALUE(p.last_close_date) AS last_close_date,
  ROUND(100 * (SAFE_DIVIDE(ANY_VALUE(p.last_close), SAFE_DIVIDE(SUM(b.value), SUM(b.shares))) - 1), 1) AS pct_vs_insider_vwap,
  ANY_VALUE(p.last_close) IS NULL AS unpriced
FROM b LEFT JOIN s USING (ticker) LEFT JOIN last_px p ON p.symbol = b.ticker
GROUP BY b.ticker, s.sold_30d, s.sellers_30d
HAVING buyers >= 3 AND bought_usd >= 250000""")
# ------------------------------------------------------------------ crypto_*
view("rrp.crypto_spot_latest", """-- latest spot reading per source and symbol (Kraken, Coinbase), with its age
SELECT * EXCEPT(rn), TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY source, symbol ORDER BY ts DESC) rn
  FROM `P.markets.crypto_spot` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)) WHERE rn = 1""")
view("rrp.crypto_core_daily", """-- the 12 core coins (Kraken): completed daily candles with MA20 / MA50 / RSI14 (Wilder)
SELECT o.symbol, DATE(o.bar_start) AS date, o.open, o.high, o.low, o.close, o.vwap, o.volume, o.trades,
       i.ma20, i.ma50, i.rsi14
FROM `P.markets.crypto_ohlc` o LEFT JOIN `P.markets.crypto_indicators_daily` i ON i.symbol = o.symbol AND i.date = DATE(o.bar_start)
WHERE o.interval_min = 1440 AND NOT o.is_open""")
view("rrp.crypto_core_hourly", """-- the 12 core coins (Kraken): hourly candles; is_open = the current unfinished hour
SELECT symbol, bar_start, open, high, low, close, vwap, volume, trades, is_open
FROM `P.markets.crypto_ohlc` WHERE interval_min = 60""")
view("rrp.crypto_all_daily", """-- daily OHLCV for every Kraken and Coinbase USD pair (full history each venue serves). base = the coin; tickers can
-- collide across projects, so check the price against rrp.crypto_market before treating two rows as one coin. is_open = today
-- Bad bars are left out: a date before bitcoin's first block (Kraken sent a 1970-01-01 PIPEUSD bar) or a price of 0 or less.
SELECT venue, pair, base, quote, date, open, high, low, close, vwap, volume, trades, is_open
FROM `P.markets.crypto_daily` WHERE date >= '2009-01-03' AND close > 0 AND open > 0 AND low > 0 AND high > 0""")
view("rrp.crypto_market", """-- CoinPaprika top 2,000 coins, one snapshot per hour (ts): price, volume, market cap, % changes, ATH and its date,
-- first_data_at (project age). Filter on ts to read one snapshot
SELECT * FROM `P.markets.crypto_market`""")
view("rrp.crypto_derivs_latest", """-- BTC and ETH perpetuals (Deribit): latest mark, funding (8h and annualized) and open interest
SELECT * EXCEPT(rn), TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min FROM (
  SELECT *, ROUND(funding_8h * 3 * 365 * 100, 2) AS funding_annualized_pct,
         ROW_NUMBER() OVER (PARTITION BY instrument ORDER BY ts DESC) rn
  FROM `P.markets.crypto_derivs` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)) WHERE rn = 1""")
view("rrp.crypto_global_latest", """-- total crypto market cap, volume and BTC dominance (CoinPaprika), latest reading
SELECT * EXCEPT(rn), TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY ts DESC) rn FROM `P.markets.crypto_global`
  WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)) WHERE rn = 1""")
view("rrp.crypto_trending_latest", """-- Coinbase trending list, latest snapshot
SELECT * FROM `P.markets.crypto_trending`
WHERE ts = (SELECT MAX(ts) FROM `P.markets.crypto_trending` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))""")
view("rrp.crypto_btc_fees_latest", """-- bitcoin network fee estimates (mempool.space, sat/vB), latest reading
SELECT * FROM `P.markets.btc_fees`
WHERE ts = (SELECT MAX(ts) FROM `P.markets.btc_fees` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))""")
view("rrp.crypto_fear_greed", """-- Crypto Fear & Greed index (alternative.me), one row per day
SELECT date, value, classification FROM `P.markets.crypto_fear_greed`""")

# ------------------------------------------------------------------ etf_flows*: US spot crypto ETF flows (Farside)
view("rrp.etf_flows", """-- US spot bitcoin, ether and solana ETF net flows from Farside, US$ millions (negative = outflow), one row per fund per
-- trading day; fund TOTAL is Farside's own daily total. The newest day can change as late funds report; refreshed hourly.
SELECT asset, date, fund, flow_usd_m, ingested_at FROM `P.markets.etf_flows` WHERE fund != 'TOTAL'""")
view("rrp.etf_flows_daily", """-- one row per asset per trading day: Farside's total, the sum of the funds (they should match), funds reporting,
-- and the run of consecutive inflow (+) or outflow (-) days ending that day
WITH d AS (
  SELECT asset, date, MAX(IF(fund = 'TOTAL', flow_usd_m, NULL)) AS total_usd_m,
         ROUND(SUM(IF(fund != 'TOTAL', flow_usd_m, 0)), 1) AS funds_sum_usd_m, COUNTIF(fund != 'TOTAL' AND flow_usd_m != 0) AS funds_with_flow
  FROM `P.markets.etf_flows` GROUP BY asset, date),
s AS (SELECT *, SIGN(total_usd_m) AS sg,
        COUNTIF(SIGN(total_usd_m) != prev_sg OR prev_sg IS NULL) OVER (PARTITION BY asset ORDER BY date) AS grp
      FROM (SELECT *, LAG(SIGN(total_usd_m)) OVER (PARTITION BY asset ORDER BY date) AS prev_sg FROM d))
SELECT asset, date, total_usd_m, funds_sum_usd_m, funds_with_flow,
       CAST(sg * ROW_NUMBER() OVER (PARTITION BY asset, grp ORDER BY date) AS INT64) AS streak_days
FROM s""")

# ------------------------------------------------------------------ funding_*: perpetual funding rates (added 2026-10-12)
view("rrp.funding_rates", """-- perpetual futures funding, every funding period: Kraken Futures (PF_ perpetuals and the old PI_ BTC/ETH inverse
-- perps; Kraken serves about a year) and Deribit BTC-PERPETUAL / ETH-PERPETUAL (hourly, back to 2019). rate_pct = the period's rate as % of notional (positive = longs pay shorts);
-- annualized_pct scales it by the instrument's own spacing between funding periods.
WITH r AS (SELECT *, TIMESTAMP_DIFF(ts, LAG(ts) OVER (PARTITION BY venue, instrument ORDER BY ts), SECOND) AS gap_s
           FROM `P.markets.funding_rates`)
SELECT venue, instrument, symbol, ts, ROUND(100 * relative_funding_rate, 6) AS rate_pct,
       ROUND(100 * relative_funding_rate * 31536000 / NULLIF(gap_s, 0), 3) AS annualized_pct, funding_rate
FROM r""")
view("rrp.funding_daily", """-- one row per coin per UTC day: average annualized funding from one source per day, in order of preference:
-- the coin's Kraken PF_ perpetuals, else Kraken's old PI_ inverse perp, else Deribit (BTC and ETH before Kraken's year)
WITH r AS (SELECT *, CASE WHEN STARTS_WITH(instrument, 'PF_') THEN 1 WHEN venue = 'deribit' THEN 3 ELSE 2 END AS pref
           FROM `P.rrp.funding_rates` WHERE annualized_pct IS NOT NULL),
k AS (SELECT *, MIN(pref) OVER (PARTITION BY symbol, DATE(ts)) AS best FROM r)
SELECT symbol, DATE(ts) AS date, ROUND(AVG(annualized_pct), 3) AS annualized_pct, COUNT(*) AS periods,
       ARRAY_AGG(DISTINCT instrument) AS instruments
FROM k WHERE pref = best
GROUP BY symbol, date""")

# ------------------------------------------------------------------ stablecoins_*
view("rrp.stablecoins_latest", """-- every stablecoin DefiLlama tracks, latest hourly reading: circulating supply now, a day, a week and a month ago
SELECT * FROM `P.markets.stablecoins`
WHERE ts = (SELECT MAX(ts) FROM `P.markets.stablecoins` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY))""")
view("rrp.stablecoins_daily", """-- stablecoin supply, one row per coin per day (the last reading of each UTC day). Every coin DefiLlama tracks from
-- the live feed (Sep 28 2026 on); USDT, USDC and DAI back to 2017 from the history table (source 'history').
WITH live AS (
  SELECT DATE(ts) AS date, id, symbol AS coin, name, peg_type, circulating AS circulating_usd, prev_day, prev_week, prev_month,
         'defillama' AS source, ts
  FROM `P.markets.stablecoins` WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id, DATE(ts) ORDER BY ts DESC) = 1)
SELECT * FROM live
UNION ALL
SELECT h.date, CASE h.coin WHEN 'USDT' THEN '1' WHEN 'USDC' THEN '2' WHEN 'DAI' THEN '5' END, h.coin, CAST(NULL AS STRING),
       'peggedUSD', h.circulating_usd, NULL, NULL, NULL, 'history', CAST(NULL AS TIMESTAMP)
FROM `P.markets.history_stablecoins` h
WHERE h.date < (SELECT MIN(date) FROM live WHERE id IN ('1', '2', '5'))""")

# ------------------------------------------------------------------ macro_* and yields
view("rrp.macro_daily", """-- every benchmark close by market date and source (rates, indexes, VIX, commodities, FX), history for look-backs.
-- role (b196, 2026-10-26) says which source to use per label: canonical = the one source to quote and count records on;
-- same = another source for the same number (VIX from FRED / Yahoo, index closes from FRED: identical on 99%+ of days),
-- used only for a date the canonical source has not posted yet; reference = a different measure under the same label,
-- kept as a cross-check, never mixed into the canonical series: FRED WTI / Brent = physical spot (posted ~4 days late)
-- against Yahoo front-month futures (median gap 1.4% over the last year), FRED FX = the New York noon buying rate
-- against Yahoo's market close (~0.2% apart); retired = LBMA gold / silver fixes (47 days, Jul-Sep 2026, feed removed).
-- measure names what the number is.
SELECT obs_date, label, source, series, close, open, high, low,
       CASE WHEN label LIKE 'UST%' THEN IF(source = 'treasury', 'canonical', 'reference')
            WHEN label = 'VIX' THEN IF(source = IF(obs_date < DATE '2026-10-29', 'cboe', 'yahoo'), 'canonical', 'same')   -- Cboe pull stopped 2026-10-29 (b252)
            WHEN label IN ('SPX', 'NDX', 'DJIA') THEN IF(source = 'yahoo', 'canonical', 'same')
            WHEN source = 'lbma' THEN 'retired'
            WHEN label IN ('WTI', 'BRENT', 'GOLD', 'SILVER', 'EURUSD', 'USDJPY') THEN IF(source = 'yahoo', 'canonical', 'reference')
            ELSE 'canonical' END AS role,
       CASE WHEN source = 'yahoo' AND label IN ('WTI', 'BRENT', 'GOLD', 'SILVER', 'COPPER', 'ES_FUT', 'NQ_FUT') THEN 'front-month future'
            WHEN source = 'fred' AND label IN ('WTI', 'BRENT') THEN 'spot (physical), posted ~4 days late'
            WHEN source = 'fred' AND STARTS_WITH(series, 'DEX') THEN 'New York noon buying rate (Fed H.10)'
            WHEN source = 'lbma' THEN 'LBMA fix'
            WHEN label LIKE 'UST%' THEN 'par yield, percent'
            WHEN label IN ('BANK_RESERVES', 'FED_ASSETS', 'ON_RRP') THEN 'Fed balance sheet, $ millions (weekly / daily)'
            ELSE 'close' END AS measure
FROM `P.markets.macro`""")
view("rrp.macro_latest", """-- newest observation per benchmark from its canonical source (rrp.macro_daily role), or from a 'same' source for a
-- date the canonical one has not posted yet (VIX intraday from Yahoo before Cboe posts the close); never a 'reference'
-- source (FRED oil spot, FRED FX noon rates), which until 2026-10-26 could win on a tie. change = vs that source's
-- previous observation. obs_date is the market date the value belongs to
WITH m AS (SELECT *, IF(role = 'canonical', 1, 2) AS pri
           FROM `P.rrp.macro_daily` WHERE obs_date > DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY) AND close IS NOT NULL
             AND role IN ('canonical', 'same')),
raw AS (SELECT source, series, obs_date, quote_time, ingested_at FROM `P.markets.macro`
        WHERE obs_date > DATE_SUB(CURRENT_DATE(), INTERVAL 40 DAY)),
c AS (SELECT m.*, r.quote_time, r.ingested_at,
             LAG(m.close) OVER (PARTITION BY m.source, m.series ORDER BY m.obs_date) AS prev_close,
             LAG(m.obs_date) OVER (PARTITION BY m.source, m.series ORDER BY m.obs_date) AS prev_date
      FROM m LEFT JOIN raw r USING (source, series, obs_date)),
best AS (SELECT * FROM c QUALIFY ROW_NUMBER() OVER (PARTITION BY label ORDER BY obs_date DESC, pri) = 1)
SELECT label, obs_date, close, prev_date, prev_close, ROUND(close - prev_close, 4) AS change,
       ROUND(100 * (close / prev_close - 1), 2) AS change_pct, source, series, quote_time,
       DATE_DIFF(CURRENT_DATE('America/New_York'), obs_date, DAY) AS age_days, ingested_at, role, measure
FROM best""")
view("rrp.yields_daily", """-- US Treasury 2Y / 10Y / 30Y yields (percent), one row per market day since 1962. Before 1990 from the history
-- table; from 1990 the official Treasury series the rrp-market job loads
SELECT date, y2, y10, y30, 'history' AS source FROM `P.markets.history_yields` WHERE date < DATE '1990-01-01'
UNION ALL
SELECT obs_date AS date,
       MAX(IF(label = 'UST2Y', close, NULL)) AS y2,
       MAX(IF(label = 'UST10Y', close, NULL)) AS y10,
       MAX(IF(label = 'UST30Y', close, NULL)) AS y30,
       'treasury' AS source
FROM `P.markets.macro`
WHERE label IN ('UST2Y', 'UST10Y', 'UST30Y') AND source = 'treasury' AND obs_date >= DATE '1990-01-01'
GROUP BY obs_date""")

# ------------------------------------------------------------------ gov_*
view("rrp.gov_cot", """-- CFTC Commitments of Traders since 2006, one row per market per week, with net positions and week-over-week change.
-- is_major (2026-10-26): the ~41 benchmark markets readers know (Treasuries, SOFR / fed funds, S&P / Nasdaq / Russell,
-- VIX, major currencies, CME bitcoin / ether, WTI / Brent / gas / RBOB / ULSD, gold / silver / copper / platinum /
-- palladium, grains, softs, livestock). Every market is loaded; the rest (basis swaps, regional power, RECs) are niche.
SELECT report, report_date, market_code, market_name, open_interest_all AS open_interest,
       REGEXP_CONTAINS(market_name, r'^(E-MINI S&P 500 -|NASDAQ MINI -|RUSSELL E-MINI -|UST (2Y|5Y|10Y) NOTE -|UST BOND -|ULTRA UST (10Y|BOND) -|EURO FX -|JAPANESE YEN -|BRITISH POUND -|CANADIAN DOLLAR -|AUSTRALIAN DOLLAR -|MEXICAN PESO -|SWISS FRANC -|VIX FUTURES -|BITCOIN - CHICAGO|ETHER CASH SETTLED - CHICAGO|SOFR-3M - CHICAGO|FED FUNDS -|CRUDE OIL, LIGHT SWEET-WTI -|WTI-PHYSICAL -|NAT GAS NYME -|GOLD - COMMODITY|SILVER - COMMODITY|COPPER- #1 -|CORN - CHICAGO|SOYBEANS - CHICAGO|WHEAT-SRW -|GASOLINE RBOB -|NY HARBOR ULSD -|BRENT LAST DAY -|SUGAR NO. 11 -|COFFEE C -|COCOA - ICE|COTTON NO. 2 -|LIVE CATTLE -|LEAN HOGS -|PLATINUM -|PALLADIUM -)') AS is_major,
       lev_money_positions_long - lev_money_positions_short AS lev_funds_net,
       asset_mgr_positions_long - asset_mgr_positions_short AS asset_mgr_net,
       dealer_positions_long_all - dealer_positions_short_all AS dealer_net,
       m_money_positions_long_all - m_money_positions_short_all AS managed_money_net,
       lev_money_positions_long - lev_money_positions_short
         - LAG(lev_money_positions_long - lev_money_positions_short) OVER w AS lev_funds_net_chg,
       m_money_positions_long_all - m_money_positions_short_all
         - LAG(m_money_positions_long_all - m_money_positions_short_all) OVER w AS managed_money_net_chg,
       open_interest_all - LAG(open_interest_all) OVER w AS open_interest_chg,
       * EXCEPT(report, report_date, market_code, market_name, raw_json, ingested_at)
FROM `P.gov.cot` WINDOW w AS (PARTITION BY market_code ORDER BY report_date)""")
view("rrp.gov_dod_awards", """-- Defense contract transactions on awards of $100M+ from USAspending (posts with roughly a 90-day delay). The $100M
-- filter is on the award, not the transaction: most rows are smaller modifications and ~11% are negative de-obligations,
-- so filter amount_usd for "large contract" facts (Weekly Flows keeps >= $250M).
SELECT action_date, recipient, amount_usd, is_new_award, mod, award_id, sub_agency, description, url
FROM `P.gov.dod_awards`""")
view("rrp.gov_war_days", """-- days with a war.gov daily contracts announcement page
SELECT contract_date, title, link, published FROM `P.gov.war_contract_days`""")

# ------------------------------------------------------------------ momentum_*: the momentum scan's screen and what happened next
view("rrp.momentum_paths", """-- every coin the momentum scan's number filters flag each morning, with its price every hour for the next 7 days.
-- Snapshot = the last CoinPaprika snapshot between 4:45 and 6:45 AM ET (what the 6:45 AM scan reads). f1 mcap $20-200M,
-- f2 vol/mcap > 1.5, f4 70%+ below an ATH set over a year ago, f5 data older than 6 months. status = cleared (all four) or
-- near_miss (three, with mcap $10-400M and vol/mcap > 0.75). hours_after 0 = the scan snapshot itself
WITH snaps AS (SELECT DISTINCT ts FROM `P.markets.crypto_market`),
scan AS (
  SELECT DATE(ts, 'America/New_York') AS scan_date, MAX(ts) AS scan_ts FROM snaps
  WHERE TIME(ts, 'America/New_York') BETWEEN '04:45:00' AND '06:45:00' GROUP BY 1),
screened AS (
  SELECT s.scan_date, s.scan_ts, m.id, m.symbol, m.name, m.rank, m.price AS scan_price, m.market_cap, m.volume_24h,
    ROUND(SAFE_DIVIDE(m.volume_24h, m.market_cap), 2) AS vol_to_mcap, m.pct_24h, m.pct_7d, m.pct_from_ath, m.ath_date, m.first_data_at,
    IFNULL(m.market_cap BETWEEN 20e6 AND 200e6, FALSE) AS f1_mcap,
    IFNULL(SAFE_DIVIDE(m.volume_24h, m.market_cap) > 1.5, FALSE) AS f2_volume,
    IFNULL(m.pct_from_ath <= -70 AND m.ath_date < DATE_SUB(s.scan_date, INTERVAL 1 YEAR), FALSE) AS f4_drawdown,
    IFNULL(m.first_data_at < DATE_SUB(s.scan_date, INTERVAL 6 MONTH), FALSE) AS f5_age
  FROM scan s JOIN `P.markets.crypto_market` m ON m.ts = s.scan_ts
  WHERE m.price > 0),
flagged AS (
  SELECT *, CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) AS passed
  FROM screened
  WHERE CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) >= 3
    AND (f1_mcap OR market_cap BETWEEN 10e6 AND 400e6)
    AND (f2_volume OR SAFE_DIVIDE(volume_24h, market_cap) > 0.75))
SELECT f.scan_date, f.scan_ts, f.id, f.symbol, f.name, f.rank,
  IF(f.passed = 4, 'cleared', 'near_miss') AS status,
  CASE WHEN f.passed = 4 THEN NULL WHEN NOT f.f1_mcap THEN 'f1_mcap' WHEN NOT f.f2_volume THEN 'f2_volume'
       WHEN NOT f.f4_drawdown THEN 'f4_drawdown' ELSE 'f5_age' END AS failed_filter,
  f.scan_price, f.market_cap, f.volume_24h, f.vol_to_mcap, f.pct_24h, f.pct_7d, f.pct_from_ath, f.ath_date, f.first_data_at,
  CAST(ROUND(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) / 3600) AS INT64) AS hours_after,
  h.ts, h.price,
  ROUND(100 * (h.price / f.scan_price - 1), 2) AS ret_pct
FROM flagged f
JOIN `P.markets.crypto_market` h
  ON h.id = f.id AND h.ts >= f.scan_ts AND h.ts < TIMESTAMP_ADD(f.scan_ts, INTERVAL 10110 MINUTE)
WHERE h.price > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.scan_date, f.id, CAST(ROUND(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) / 3600) AS INT64)
                           ORDER BY ABS(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) - 3600 * ROUND(TIMESTAMP_DIFF(h.ts, f.scan_ts, SECOND) / 3600))) = 1""")
view("rrp.ref_pegged", """-- coins that track a dollar or gold price: left out of every movers list and momentum read. kind = usd / eur / gold
SELECT symbol, kind FROM UNNEST([
  STRUCT('USDT' AS symbol, 'usd' AS kind), ('USDC', 'usd'), ('USDE', 'usd'), ('DAI', 'usd'), ('FDUSD', 'usd'), ('PYUSD', 'usd'),
  ('USD1', 'usd'), ('USDS', 'usd'), ('TUSD', 'usd'), ('USDP', 'usd'), ('GUSD', 'usd'), ('RLUSD', 'usd'), ('USDD', 'usd'),
  ('BUSD', 'usd'), ('FRAX', 'usd'), ('LUSD', 'usd'), ('USDG', 'usd'), ('USDTB', 'usd'), ('USD0', 'usd'), ('BUIDL', 'usd'),
  ('USYC', 'usd'), ('SUSDE', 'usd'), ('SUSDS', 'usd'), ('GHO', 'usd'), ('CRVUSD', 'usd'), ('USDF', 'usd'), ('USDX', 'usd'),
  ('EURC', 'eur'), ('EURS', 'eur'), ('PAXG', 'gold'), ('XAUT', 'gold')])""")

view("rrp.crypto_24h", """-- the 24h figure for every Kraken core coin: last trade vs the price 24 hours earlier, from the hourly bars (the bar
-- that held that moment: its open when the moment fell in the first half of the hour, else its close). high/low over
-- the same 24 hours. coinbase_gap_pct = Coinbase vs Kraken last (BTC, ETH); price_check_pct = CoinPaprika's
-- aggregated price vs Kraken last. ok = fresh (age_min <= 30), a full set of bars, and both checks within 1.5%.
WITH s AS (SELECT symbol, ts, last, age_min FROM `P.rrp.crypto_spot_latest` WHERE source = 'kraken'),
h AS (SELECT symbol, bar_start, open, high, low, close FROM `P.rrp.crypto_core_hourly`
      WHERE bar_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)),
w AS (SELECT s.symbol, s.ts, s.last, s.age_min,
        ANY_VALUE(IF(h.bar_start = TIMESTAMP_TRUNC(TIMESTAMP_SUB(s.ts, INTERVAL 24 HOUR), HOUR),
                     IF(EXTRACT(MINUTE FROM s.ts) < 30, h.open, h.close), NULL)) AS price_24h_ago,
        MAX(h.high) AS hi, MIN(h.low) AS lo, COUNT(*) AS bars
      FROM s JOIN h ON h.symbol = s.symbol AND h.bar_start >= TIMESTAMP_TRUNC(TIMESTAMP_SUB(s.ts, INTERVAL 24 HOUR), HOUR)
      GROUP BY 1, 2, 3, 4),
cp AS (SELECT symbol, price FROM `P.rrp.crypto_market`
       WHERE ts = (SELECT MAX(ts) FROM `P.rrp.crypto_market` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
       QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY rank) = 1),
cb AS (SELECT symbol, last FROM `P.rrp.crypto_spot_latest` WHERE source = 'coinbase_exchange')
SELECT w.symbol, w.ts, w.age_min, w.last, w.price_24h_ago,
       ROUND(100 * (w.last / w.price_24h_ago - 1), 2) AS pct_24h,
       ROUND(w.last - w.price_24h_ago, 6) AS change_24h,
       GREATEST(w.hi, w.last) AS high_24h, LEAST(w.lo, w.last) AS low_24h, w.bars,
       ROUND(100 * (cb.last / w.last - 1), 3) AS coinbase_gap_pct,
       ROUND(100 * (cp.price / w.last - 1), 3) AS price_check_pct,
       w.age_min <= 30 AND w.bars >= 24 AND w.price_24h_ago IS NOT NULL
         AND IFNULL(ABS(cb.last / w.last - 1) <= 0.015, TRUE) AND IFNULL(ABS(cp.price / w.last - 1) <= 0.015, TRUE) AS ok
FROM w LEFT JOIN cp USING (symbol) LEFT JOIN cb USING (symbol)""")

view("rrp.crypto_movers", """-- the movers list: newest CoinPaprika snapshot, top 200 by market cap, $10M+ 24h volume, pegged coins left out,
-- ordered by the size of the 24h move. pct_24h = price now vs the snapshot 24 hours earlier (+/- 90 min), the same
-- definition as rrp.crypto_24h (CoinPaprika's own pct_24h runs about an hour behind; it is kept as paprika_pct_24h).
-- move_rank 1 = biggest move either way. age_min = snapshot age; read nothing when it is over 180.
WITH m AS (SELECT * FROM `P.rrp.crypto_market` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)),
now_ AS (SELECT * FROM m WHERE ts = (SELECT MAX(ts) FROM m)),
then_ts AS (SELECT ARRAY_AGG(ts ORDER BY ABS(TIMESTAMP_DIFF(ts, TIMESTAMP_SUB((SELECT MAX(ts) FROM m), INTERVAL 24 HOUR), MINUTE)) LIMIT 1)[OFFSET(0)] AS t
            FROM (SELECT DISTINCT ts FROM m)
            WHERE ABS(TIMESTAMP_DIFF(ts, TIMESTAMP_SUB((SELECT MAX(ts) FROM m), INTERVAL 24 HOUR), MINUTE)) <= 90),
then_ AS (SELECT id, price FROM m WHERE ts = (SELECT t FROM then_ts))
SELECT n.ts, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), n.ts, MINUTE) AS age_min, (SELECT t FROM then_ts) AS ts_24h_ago,
       n.id, n.symbol, n.name, n.rank, n.price, t.price AS price_24h_ago,
       ROUND(100 * COALESCE(n.price / t.price - 1, n.pct_24h / 100), 2) AS pct_24h, n.pct_24h AS paprika_pct_24h,
       n.pct_7d, n.volume_24h, n.market_cap,
       ROW_NUMBER() OVER (ORDER BY ABS(COALESCE(n.price / t.price - 1, n.pct_24h / 100)) DESC) AS move_rank
FROM now_ n LEFT JOIN then_ t USING (id)
WHERE n.rank <= 200 AND n.volume_24h >= 10e6 AND UPPER(n.symbol) NOT IN (SELECT symbol FROM `P.rrp.ref_pegged`)""")

view("rrp.momentum_screen", """-- the daily momentum scan's number filters on the newest CoinPaprika snapshot (all 2,000 coins): f1 market cap
-- $20M-$200M, f2 24h volume over 1.5x market cap, f4 70%+ below an all-time high set more than a year ago, f5 listed
-- 6+ months. passed = how many of the four; 4 = clears the numbers, 3 = near miss (shown only when the failed filter
-- was close: cap $10M-$400M and volume over 0.75x). The catalyst (filter 3) is judged by the task, not here.
WITH snap AS (SELECT * FROM `P.rrp.crypto_market` WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
              QUALIFY ts = MAX(ts) OVER ()),
s AS (SELECT ts AS snapshot_ts, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), ts, MINUTE) AS age_min, id, symbol, name, rank, price,
        market_cap, volume_24h, ROUND(SAFE_DIVIDE(volume_24h, market_cap), 2) AS vol_to_mcap, pct_24h, pct_7d, ath_price,
        ath_date, pct_from_ath, first_data_at,
        IFNULL(market_cap BETWEEN 20e6 AND 200e6, FALSE) AS f1_mcap,
        IFNULL(SAFE_DIVIDE(volume_24h, market_cap) > 1.5, FALSE) AS f2_volume,
        IFNULL(pct_from_ath <= -70 AND ath_date < DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR), FALSE) AS f4_drawdown,
        IFNULL(first_data_at < DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), FALSE) AS f5_age
      FROM snap WHERE UPPER(symbol) NOT IN (SELECT symbol FROM `P.rrp.ref_pegged`))
SELECT *, CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) AS passed
FROM s
WHERE CAST(f1_mcap AS INT64) + CAST(f2_volume AS INT64) + CAST(f4_drawdown AS INT64) + CAST(f5_age AS INT64) >= 3
  AND (f1_mcap OR market_cap BETWEEN 10e6 AND 400e6)
  AND (f2_volume OR SAFE_DIVIDE(volume_24h, market_cap) > 0.75)""")

view("rrp.stablecoin_supply", """-- stablecoin supply now and its change over a day, a week and a month, per coin (DefiLlama, hourly), plus a TOTAL
-- row for every dollar-pegged coin (changes summed over the coins that have the earlier reading). frozen = the reading
-- equals the day-ago reading within $5 (a stuck source, not a flat day); rank 1 = largest coin. as_of_et = New York date.
WITH c AS (SELECT ts, id, symbol AS coin, name, peg_type, circulating AS supply_usd,
                  circulating - prev_day AS change_1d_usd, circulating - prev_week AS change_7d_usd,
                  circulating - prev_month AS change_30d_usd, prev_week
           FROM `P.rrp.stablecoins_latest` WHERE peg_type = 'peggedUSD'),
u AS (SELECT * FROM c
      UNION ALL
      SELECT MAX(ts), 'total', 'TOTAL', 'all dollar stablecoins', 'peggedUSD', SUM(supply_usd), SUM(change_1d_usd),
             SUM(change_7d_usd), SUM(change_30d_usd), SUM(prev_week) FROM c)
SELECT ts, DATE(ts, 'America/New_York') AS as_of_et, id, coin, name, supply_usd, change_1d_usd, change_7d_usd, change_30d_usd,
       ROUND(100 * SAFE_DIVIDE(change_7d_usd, prev_week), 2) AS change_7d_pct,
       coin != 'TOTAL' AND ABS(change_1d_usd) < 5 AS frozen,
       IF(coin = 'TOTAL', 0, ROW_NUMBER() OVER (PARTITION BY coin = 'TOTAL' ORDER BY supply_usd DESC)) AS rank
FROM u""")

view("rrp.etf_flows_latest", """-- spot crypto ETF flows, newest day per asset before today (New York): today's flows are never final until the next
-- morning. A day with a zero total and no fund reporting is skipped as not posted yet. streak_days: + inflow run, - outflow run. top_fund / top_fund_usd_m = the day's largest absolute
-- fund flow; ibit / fbtc for bitcoin. week_usd_m = Monday to this day; largest_since = the last day with a flow at least
-- this big in the same direction (NULL = largest on record). may_be_partial = read before 10 AM New York time on the
-- next weekday: Farside fills in late funds overnight and stores a missing fund as 0, so the day can still change.
WITH d AS (SELECT *, LAG(total_usd_m) OVER (PARTITION BY asset ORDER BY date) AS prev_day_usd_m,
                  ROUND(SUM(total_usd_m) OVER (PARTITION BY asset, DATE_TRUNC(date, WEEK(MONDAY)) ORDER BY date), 1) AS week_usd_m,
                  ROUND(SUM(total_usd_m) OVER (PARTITION BY asset ORDER BY UNIX_DATE(date) RANGE BETWEEN 29 PRECEDING AND CURRENT ROW), 1) AS last_30d_usd_m,
                  ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS age_rank
           FROM `P.rrp.etf_flows_daily` WHERE NOT (IFNULL(total_usd_m, 0) = 0 AND funds_with_flow = 0)
             AND date < CURRENT_DATE('America/New_York')),
l AS (SELECT * FROM d WHERE age_rank = 1),
big AS (SELECT l.asset, MAX(d.date) AS largest_since FROM l JOIN d ON d.asset = l.asset AND d.date < l.date
          AND SIGN(d.total_usd_m) = SIGN(l.total_usd_m) AND ABS(d.total_usd_m) >= ABS(l.total_usd_m) GROUP BY l.asset),
fu AS (SELECT f.asset, ARRAY_AGG(STRUCT(f.fund, f.flow_usd_m) ORDER BY ABS(f.flow_usd_m) DESC LIMIT 1)[OFFSET(0)] AS top,
              MAX(IF(f.fund = 'IBIT', f.flow_usd_m, NULL)) AS ibit_usd_m, MAX(IF(f.fund = 'FBTC', f.flow_usd_m, NULL)) AS fbtc_usd_m
       FROM `P.rrp.etf_flows` f JOIN l ON f.asset = l.asset AND f.date = l.date GROUP BY f.asset)
SELECT l.asset, l.date, l.total_usd_m, l.prev_day_usd_m, l.streak_days, l.week_usd_m, l.last_30d_usd_m, big.largest_since,
       fu.top.fund AS top_fund, fu.top.flow_usd_m AS top_fund_usd_m, fu.ibit_usd_m, fu.fbtc_usd_m,
       l.funds_with_flow,
       CURRENT_DATETIME('America/New_York') < DATETIME(DATE_ADD(l.date, INTERVAL IF(EXTRACT(DAYOFWEEK FROM l.date) = 6, 3, 1) DAY),
                                                      TIME '10:00:00') AS may_be_partial
FROM l LEFT JOIN big USING (asset) LEFT JOIN fu USING (asset)""")

view("rrp.calendar", """-- one row per day, 2006 to 30 days ahead: whether US stock markets traded (SPY has a bar) and whether EDGAR posted
-- filings (sec.form4 has rows), and the previous such day. Days the data has not reached yet (today and later) use the
-- published 2026-2027 NYSE and federal holiday lists; *_confirmed = TRUE when the day is backed by data.
WITH spy AS (SELECT DISTINCT date FROM `P.rrp.equity_daily` WHERE symbol = 'SPY'),
ed AS (SELECT DISTINCT filed AS date FROM `P.sec.form4`),
known AS (SELECT MAX(date) AS spy_max FROM spy),
k2 AS (SELECT MAX(date) AS ed_max FROM ed),
days AS (SELECT d AS date FROM UNNEST(GENERATE_DATE_ARRAY('2006-01-01', DATE_ADD(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY))) d),
x AS (SELECT days.date, EXTRACT(DAYOFWEEK FROM days.date) BETWEEN 2 AND 6 AS weekday,
        CASE WHEN days.date <= (SELECT spy_max FROM known) THEN spy.date IS NOT NULL END AS nyse_known,
        CASE WHEN days.date <= (SELECT ed_max FROM k2) THEN ed.date IS NOT NULL END AS edgar_known
      FROM days LEFT JOIN spy USING (date) LEFT JOIN ed USING (date)),
y AS (SELECT *, COALESCE(nyse_known, weekday AND date NOT IN UNNEST([DATE '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
          '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25', '2027-01-01', '2027-01-18',
          '2027-02-15', '2027-03-26', '2027-05-31', '2027-06-18', '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24'])) AS nyse_open,
        COALESCE(edgar_known, weekday AND date NOT IN UNNEST([DATE '2026-01-01', '2026-01-19', '2026-02-16', '2026-05-25',
          '2026-06-19', '2026-07-03', '2026-09-07', '2026-10-12', '2026-11-11', '2026-11-26', '2026-12-25', '2027-01-01',
          '2027-01-18', '2027-02-15', '2027-05-31', '2027-06-18', '2027-07-05', '2027-09-06', '2027-10-11', '2027-11-11',
          '2027-11-25', '2027-12-24'])) AS edgar_open
      FROM x)
SELECT date, FORMAT_DATE('%a', date) AS dow, weekday, nyse_open, nyse_known IS NOT NULL AS nyse_confirmed,
       edgar_open, edgar_known IS NOT NULL AS edgar_confirmed,
       LAST_VALUE(IF(nyse_open, date, NULL) IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_nyse_day,
       LAST_VALUE(IF(edgar_open, date, NULL) IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_edgar_day
FROM y""")

view("rrp.crypto_records", """-- for each Kraken core coin, its newest completed UTC day measured against its own history (look-back facts):
-- day_pct = close vs previous close; biggest_move_since = last day with a move at least this large either way;
-- highest_close_since / lowest_close_since = last close at or above / at or below today's (NULL = a record since
-- history_from); above_ma50_days = consecutive closes on today's side of the 50-day average (+ above, - below);
-- ret_7d / ret_30d close to close.
WITH d AS (SELECT symbol, date, close, ma50, ROUND(100 * (close / LAG(close) OVER (PARTITION BY symbol ORDER BY date) - 1), 2) AS day_pct,
                  LAG(close, 7) OVER (PARTITION BY symbol ORDER BY date) AS c7, LAG(close, 30) OVER (PARTITION BY symbol ORDER BY date) AS c30,
                  MIN(date) OVER (PARTITION BY symbol) AS history_from
           FROM `P.rrp.crypto_core_daily`),
s AS (SELECT *, COUNTIF(side != prev_side OR prev_side IS NULL) OVER (PARTITION BY symbol ORDER BY date) AS grp
      FROM (SELECT *, SIGN(close - ma50) AS side, LAG(SIGN(close - ma50)) OVER (PARTITION BY symbol ORDER BY date) AS prev_side FROM d)),
r AS (SELECT *, CAST(side * ROW_NUMBER() OVER (PARTITION BY symbol, grp ORDER BY date) AS INT64) AS ma50_streak FROM s),
l AS (SELECT * FROM r QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1),
x AS (SELECT l.symbol,
        MAX(IF(ABS(d.day_pct) >= ABS(l.day_pct), d.date, NULL)) AS biggest_move_since,
        MAX(IF(d.close >= l.close, d.date, NULL)) AS highest_close_since,
        MAX(IF(d.close <= l.close, d.date, NULL)) AS lowest_close_since
      FROM l JOIN d ON d.symbol = l.symbol AND d.date < l.date GROUP BY l.symbol)
SELECT l.symbol, l.date, l.close, l.day_pct, x.biggest_move_since, x.highest_close_since, x.lowest_close_since, l.history_from,
       l.ma50, IF(l.ma50 IS NULL, NULL, l.ma50_streak) AS above_ma50_days,
       ROUND(100 * (l.close / l.c7 - 1), 2) AS ret_7d, ROUND(100 * (l.close / l.c30 - 1), 2) AS ret_30d
FROM l JOIN x USING (symbol)""")

view("rrp.yields_records", """-- US Treasury 2Y / 10Y / 30Y: newest close, its change in basis points vs the previous market day and since the last
-- close of the prior year, and the last day the yield closed at or above (highest_since) / at or below (lowest_since)
-- today's level (NULL = a record in the data, which starts in 1962).
WITH y AS (SELECT date, tenor, value, LAG(value) OVER (PARTITION BY tenor ORDER BY date) AS prev,
                  LAST_VALUE(IF(EXTRACT(YEAR FROM date) < EXTRACT(YEAR FROM CURRENT_DATE()), value, NULL) IGNORE NULLS)
                    OVER (PARTITION BY tenor ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prior_year_close
           FROM `P.rrp.yields_daily`, UNNEST([STRUCT('2Y' AS tenor, y2 AS value), ('10Y', y10), ('30Y', y30)])
           WHERE value IS NOT NULL),
l AS (SELECT * FROM y QUALIFY ROW_NUMBER() OVER (PARTITION BY tenor ORDER BY date DESC) = 1),
x AS (SELECT l.tenor, MAX(IF(y.value >= l.value, y.date, NULL)) AS highest_since, MAX(IF(y.value <= l.value, y.date, NULL)) AS lowest_since
      FROM l JOIN y ON y.tenor = l.tenor AND y.date < l.date GROUP BY l.tenor)
SELECT l.tenor, l.date, l.value AS yield_pct, ROUND(100 * (l.value - l.prev), 1) AS change_bp,
       ROUND(100 * (l.value - l.prior_year_close), 1) AS ytd_bp, x.highest_since, x.lowest_since
FROM l JOIN x USING (tenor)""")

view("rrp.fear_greed_latest", """-- Crypto Fear & Greed: newest day, its classification, how many consecutive days it has held that classification,
-- and the reading a week earlier
WITH f AS (SELECT *, LAG(value, 7) OVER (ORDER BY date) AS value_7d_ago,
                  COUNTIF(classification != prev OR prev IS NULL) OVER (ORDER BY date) AS grp
           FROM (SELECT *, LAG(classification) OVER (ORDER BY date) AS prev FROM `P.rrp.crypto_fear_greed`))
SELECT date, value, classification, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY date) AS days_in_classification, value_7d_ago
FROM f QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1""")

view("rrp.insider_filings_live", r"""-- the Form 4 screen, computed live (~3.2 GB a read; tasks read rrp.insider_filings, the nightly copy): one row per filing and direction (P = buys, S = sales), every EDGAR day since 2006. Filter on filed.
-- Joint filers are counted once; amended filings (4/A) are kept and marked. Flags come from the filing's footnotes and
-- mean CHECK, not drop (they cover the whole filing): tax_cover, plan_note, fx_note, off_market (not a market trade:
-- placement, offering, conversion, tender, repurchase, merger, gift, distribution ...), formula_price (price set by a
-- formula), mixed_classes (lines priced 3x+ apart), value_suspect ($1B+), no_ticker, late_reported (trade 30+ days
-- before filing). flag_context = the words around the first off_market / formula match.
-- Roles: is_ceo, is_cfo, is_c_suite (CEO, CFO, COO, President - not a vice president), is_officer_or_director.
-- issuer_buyers_5d / issuer_buy_usd_5d = distinct insiders and dollars in clean buys at the same ticker filed in the 5
-- days ending this filing's date (the cluster test).
-- buy_bar = a clean buy, none of the drop flags, and $100K+ or by the CEO / CFO or part of a 2+ insider cluster of $25K+.
-- sale_bar = a non-plan sale, none of the drop flags, and $500K+ or 25%+ of the stake or C-suite $250K+.
-- buy_type (buys only, added 2026-10-19) labels every purchase instead of dropping it: open_market (the only kind that can clear
-- buy_bar), ipo_purchase, negotiated (purchase/subscription agreements, private placements, totals within $50 of a round million), block_trade
-- (a matching sale at the same ticker, date, shares and price), other_off_market (conversions, tenders, formula prices ...),
-- non_usd (a currency in the footnotes or a price 1.5x+ from the market close; currency_note names it when stated),
-- data_mismatch ($1B+, or an otherwise open-market buy over 25% of market cap or with no market price, priced 10x+ the ticker's lowest Form 4 price within 30 days: fix the data), internal_transfer (trust / estate moves), fund_seed,
-- plan_purchase, unpriced. pct_mkt_cap and px_ratio (Form 4 price / market close) come from insider_buys_sized / _priced.
-- usd_stated = the footnotes say the price was already converted to dollars (then not non_usd on the footnote alone).
-- fx_currency / value_usd_fx: a non_usd buy converted at the FRED rate on (or up to 10 days before) the trade date; NULL when
-- the currency can't be established. The fact engine counts open_market value_usd plus non_usd value_usd_fx.
WITH raw AS (
  SELECT *, insider LIKE '%;%' AS joint,
    ROW_NUMBER() OVER (PARTITION BY filed, accession, code, CAST(tx_date AS STRING), CAST(shares AS STRING), IFNULL(CAST(price AS STRING), ''),
                       IFNULL(CAST(owned_after AS STRING), '') ORDER BY direct_indirect) AS rn
  FROM `P.rrp.insider_trades` WHERE form IN ('4', '4/A') AND code IN ('P', 'S')),
t AS (SELECT * FROM raw WHERE NOT (joint AND rn > 1)),
clean AS (SELECT DISTINCT filed, accession FROM `P.rrp.insider_buys`),
f AS (
  SELECT t.filed, t.form, t.accession, t.ticker, t.code, t.source, ANY_VALUE(t.issuer) AS issuer, ANY_VALUE(t.issuer_cik) AS issuer_cik,
    ANY_VALUE(t.insider) AS insider, ANY_VALUE(t.role) AS role, MIN(t.tx_date) AS first_tx, MAX(t.tx_date) AS last_tx,
    SUM(t.shares) AS shares, ROUND(SAFE_DIVIDE(SUM(ABS(t.value)), SUM(t.shares)), 4) AS avg_price, ROUND(SUM(ABS(t.value))) AS value_usd,
    IF(t.code = 'P', MAX(t.owned_after), MIN(t.owned_after)) AS owned_after,
    ROUND(100 * SAFE_DIVIDE(SUM(t.shares), SUM(t.shares) + MIN(t.owned_after)), 1) AS pct_of_stake,
    LOGICAL_OR(IFNULL(t.plan_10b5_1, FALSE)) AS plan, LOGICAL_OR(IFNULL(t.private_placement, FALSE)) AS private_placement,
    LOGICAL_OR(IFNULL(t.non_usd_suspect, FALSE)) AS non_usd, SUM(t.value) IS NULL AS unpriced,
    IFNULL(SAFE_DIVIDE(MAX(NULLIF(t.price, 0)), MIN(NULLIF(t.price, 0))) > 3, FALSE) AS mixed_classes,
    COUNTIF(t.joint AND t.rn = 1) > 0 AS joint_filing,
    LOWER(STRING_AGG(DISTINCT IFNULL(t.footnotes, ''), ' ')) AS fn,
    SUBSTR(STRING_AGG(DISTINCT IFNULL(t.footnotes, ''), ' || '), 1, 300) AS footnote
  FROM t GROUP BY t.filed, t.form, t.accession, t.ticker, t.code, t.source),
g AS (
  SELECT f.* EXCEPT (fn),
    (f.code = 'P' AND f.form = '4' AND c.accession IS NOT NULL) AS clean_buy,
    REGEXP_CONTAINS(f.fn, r'tax withholding|withholding obligation|withholding tax|sell-to-cover|sell to cover|tax obligation|tax liabilit|to cover tax') AS tax_cover,
    REGEXP_CONTAINS(f.fn, r'10b5-1|trading plan') AS plan_note,
    REGEXP_CONTAINS(f.fn, r'denominated in|exchange rate|hong kong dollar|shekel|canadian dollar|euro|sterling|renminbi| yen|brazilian real|\bbrl\b|\bars\b') AS fx_note,
    REGEXP_CONTAINS(f.fn, r'FN_OFF') AS off_market,
    REGEXP_CONTAINS(f.fn, r'FN_FORMULA') AS formula_price,
    REGEXP_CONTAINS(f.fn, r'dividend reinvest|reinvestment of dividends|stock purchase plan|direct stock purchase|in lieu of cash|fees in stock|retainer in stock') AS plan_purchase,
    REGEXP_CONTAINS(f.fn, r'substitute assets|substitution of assets|irrevocable trust|estate planning|grantor retained|transferred (from|to) (a|the|his|her)|in exchange for') AS transfer_note,
    REGEXP_CONTAINS(f.fn, r'investment company registered|seed (capital|investment|money)|initial capital') AS seed_note,
    REGEXP_CONTAINS(f.fn, r'initial public offering|\bipo\b|public offering|underwrit|concurrent (private )?placement') AS ipo_note,
    REGEXP_CONTAINS(f.fn, r'purchase agreement|subscription agreement|privately negotiated|private (placement|transaction|sale)|were issued|issued pursuant|block trade|directly from the (company|issuer)') AS negotiated_note,
    CASE WHEN REGEXP_CONTAINS(f.fn, r'brazilian real|\breais\b|\bbrl\b|r\$') THEN 'BRL'
         WHEN REGEXP_CONTAINS(f.fn, r'mexican peso|\bmxn\b') THEN 'MXN'
         WHEN REGEXP_CONTAINS(f.fn, r'canadian dollar|\bcad\b|c\$') THEN 'CAD'
         WHEN REGEXP_CONTAINS(f.fn, r'hong kong dollar|\bhkd\b|hk\$') THEN 'HKD'
         WHEN REGEXP_CONTAINS(f.fn, r'\beuros?\b|\beur\b|€ ?[0-9]') THEN 'EUR'
         WHEN REGEXP_CONTAINS(f.fn, r'japanese yen|\byen\b|\bjpy\b') THEN 'JPY' END AS currency_note,
    REGEXP_CONTAINS(f.fn, r'convert(ed|ing)? (in)?to u\.?s|been converted|converted (from|at|using)|= ?us\$') AS usd_stated,
    REGEXP_EXTRACT(f.fn, r'.{0,90}(?:FN_OFF|FN_FORMULA).{0,110}') AS flag_context,
    f.value_usd >= 1e9 AS value_suspect,
    f.avg_price > 10 * MIN(f.avg_price) OVER (PARTITION BY f.ticker ORDER BY UNIX_DATE(f.filed) RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING) AS price_outlier,
    NOT REGEXP_CONTAINS(IFNULL(f.ticker, ''), r'^[A-Z]{1,5}([.-][A-Z])?$') OR f.ticker IN ('NONE', 'NA') AS no_ticker,
    f.form != '4' AS amendment,
    DATE_DIFF(f.filed, f.first_tx, DAY) > 30 AS late_reported,
    REGEXP_CONTAINS(LOWER(f.role), r'\bceo\b|chief executive') AS is_ceo,
    REGEXP_CONTAINS(LOWER(f.role), r'\bcfo\b|chief financial') AS is_cfo,
    REGEXP_CONTAINS(LOWER(f.role), r'\bceo\b|chief executive|\bcfo\b|chief financial|\bcoo\b|chief operating')
      OR (REGEXP_CONTAINS(LOWER(f.role), r'\bpresident\b') AND NOT REGEXP_CONTAINS(LOWER(f.role), r'vice[ -]president|\bvp\b')) AS is_c_suite,
    REGEXP_CONTAINS(LOWER(IFNULL(f.role, '')), r'chief|ceo|cfo|coo|president|chair|director|officer|evp|svp|general counsel') AS is_officer_or_director,
    CONCAT('https://www.sec.gov/Archives/edgar/data/', CAST(SAFE_CAST(SUBSTR(f.accession, 1, 10) AS INT64) AS STRING), '/', f.accession, '/',
      SUBSTR(f.accession, 1, 10), '-', SUBSTR(f.accession, 11, 2), '-', SUBSTR(f.accession, 13, 6), '-index.html') AS link
  FROM f LEFT JOIN clean c ON c.accession = f.accession AND c.filed = f.filed AND f.code = 'P'),
px AS (SELECT accession, LOGICAL_OR(IFNULL(price_mismatch, FALSE)) AS price_mismatch, MAX(px_ratio) AS px_ratio
       FROM `P.rrp.insider_buys_priced` GROUP BY accession),
sz AS (SELECT accession, SUM(buy_pct_of_mkt_cap) AS pct_mkt_cap FROM `P.rrp.insider_buys_sized` GROUP BY accession),
blk AS (SELECT DISTINCT b.accession FROM t b JOIN (SELECT DISTINCT ticker, tx_date, shares, price FROM t WHERE code = 'S') s
          ON s.ticker = b.ticker AND s.tx_date = b.tx_date AND s.shares = b.shares AND s.price = b.price WHERE b.code = 'P'),
fxr AS (SELECT CASE label WHEN 'USDBRL' THEN 'BRL' WHEN 'USDMXN' THEN 'MXN' WHEN 'USDHKD' THEN 'HKD' WHEN 'USDCAD' THEN 'CAD'
                          WHEN 'USDJPY' THEN 'JPY' WHEN 'EURUSD' THEN 'EUR' END AS ccy,
               obs_date, IF(label = 'EURUSD', 1 / close, close) AS per_usd
        FROM `P.rrp.macro_daily` WHERE source = 'fred' AND close > 0
          AND label IN ('USDBRL', 'USDMXN', 'USDHKD', 'USDCAD', 'USDJPY', 'EURUSD')),
-- the currency a non-dollar buy was priced in: the one its footnotes name, when the price fits that rate (allowing for ADS ratios
-- of 1 to 100 shares), or with no footnote a far-from-1 rate (BRL, MXN, HKD, JPY) the Form 4 price is within 8% of one-for-one.
conv AS (SELECT g.accession, g.ticker, g.filed, r.ccy, r.per_usd
         FROM g LEFT JOIN px ON px.accession = g.accession
         JOIN fxr r ON r.obs_date BETWEEN DATE_SUB(g.first_tx, INTERVAL 10 DAY) AND g.first_tx
         WHERE g.code = 'P' AND NOT g.usd_stated
           AND ((r.ccy = g.currency_note AND (px.px_ratio IS NULL OR EXISTS (SELECT 1 FROM UNNEST([1, 2, 3, 4, 5, 8, 10, 20, 25, 40, 100]) AS k
                                                                             WHERE ABS(SAFE.LN(SAFE_DIVIDE(r.per_usd, px.px_ratio * k))) < 0.08)))
             OR (g.currency_note IS NULL AND r.ccy IN ('BRL', 'MXN', 'HKD', 'JPY') AND ABS(SAFE.LN(SAFE_DIVIDE(r.per_usd, px.px_ratio))) < 0.08))
         QUALIFY ROW_NUMBER() OVER (PARTITION BY g.accession, g.ticker, g.filed ORDER BY r.obs_date DESC) = 1),
bd AS (SELECT DISTINCT ticker, filed, insider, accession, value_usd FROM g
       WHERE clean_buy AND NOT off_market AND NOT formula_price AND NOT plan_purchase AND NOT value_suspect AND NOT no_ticker),
cl AS (SELECT a.ticker, a.filed, COUNT(DISTINCT b.insider) AS issuer_buyers_5d, ROUND(SUM(b.value_usd)) AS issuer_buy_usd_5d
       FROM (SELECT DISTINCT ticker, filed FROM bd) a JOIN bd b ON b.ticker = a.ticker AND b.filed BETWEEN DATE_SUB(a.filed, INTERVAL 4 DAY) AND a.filed
       GROUP BY a.ticker, a.filed),
fin AS (
SELECT g.*, cl.issuer_buyers_5d, cl.issuer_buy_usd_5d, ROUND(sz.pct_mkt_cap, 3) AS pct_mkt_cap, px.px_ratio, conv.ccy AS conv_ccy, conv.per_usd AS conv_rate,
  CASE WHEN g.code != 'P' THEN NULL
       WHEN g.transfer_note THEN 'internal_transfer'
       WHEN g.seed_note THEN 'fund_seed'
       WHEN g.plan_purchase OR g.plan THEN 'plan_purchase'
       WHEN g.value_suspect THEN 'data_mismatch'
       WHEN g.ipo_note THEN 'ipo_purchase'
       WHEN blk.accession IS NOT NULL THEN 'block_trade'
       WHEN g.negotiated_note OR g.private_placement OR (g.value_usd >= 1e6 AND ABS(g.value_usd - ROUND(g.value_usd, -6)) <= 50) THEN 'negotiated'
       WHEN g.off_market OR g.formula_price THEN 'other_off_market'
       WHEN IFNULL(sz.pct_mkt_cap, 0) > 25 OR (IFNULL(g.price_outlier, FALSE) AND px.px_ratio IS NULL) THEN 'data_mismatch'
       WHEN ((g.non_usd OR g.fx_note OR g.currency_note IS NOT NULL) AND NOT g.usd_stated) OR IFNULL(px.price_mismatch, FALSE) THEN 'non_usd'
       WHEN g.unpriced OR g.no_ticker THEN 'unpriced'
       ELSE 'open_market' END AS buy_type,
  g.clean_buy AND NOT (g.off_market OR g.formula_price OR g.plan_purchase OR g.value_suspect OR g.no_ticker OR g.non_usd OR g.fx_note OR g.unpriced)
    AND NOT (g.transfer_note OR g.seed_note OR g.ipo_note OR g.negotiated_note OR g.currency_note IS NOT NULL
             OR IFNULL(px.price_mismatch, FALSE) OR IFNULL(sz.pct_mkt_cap, 0) > 25 OR (IFNULL(g.price_outlier, FALSE) AND px.px_ratio IS NULL) OR blk.accession IS NOT NULL
             OR (g.value_usd >= 1e6 AND ABS(g.value_usd - ROUND(g.value_usd, -6)) <= 50))
    AND (g.value_usd >= 100000 OR g.is_ceo OR g.is_cfo
         OR (IFNULL(cl.issuer_buyers_5d, 0) >= 2 AND IFNULL(cl.issuer_buy_usd_5d, 0) >= 25000 AND g.is_officer_or_director)) AS buy_bar,
  g.code = 'S' AND NOT (g.plan OR g.plan_note OR g.tax_cover OR g.off_market OR g.formula_price OR g.value_suspect OR g.no_ticker
                        OR g.non_usd OR g.fx_note OR g.unpriced OR g.amendment)
    AND (g.value_usd >= 500000 OR g.pct_of_stake >= 25 OR (g.is_c_suite AND g.value_usd >= 250000)) AS sale_bar
FROM g LEFT JOIN cl ON cl.ticker = g.ticker AND cl.filed = g.filed AND g.code = 'P'
LEFT JOIN px ON px.accession = g.accession AND g.code = 'P'
LEFT JOIN sz ON sz.accession = g.accession AND g.code = 'P'
LEFT JOIN blk ON blk.accession = g.accession AND g.code = 'P'
LEFT JOIN conv ON conv.accession = g.accession AND conv.ticker = g.ticker AND conv.filed = g.filed AND g.code = 'P')
SELECT fin.* EXCEPT (conv_ccy, conv_rate), IF(buy_type = 'non_usd', conv_ccy, NULL) AS fx_currency,
  IF(buy_type = 'non_usd' AND conv_ccy IS NOT NULL, ROUND(value_usd / conv_rate), NULL) AS value_usd_fx
FROM fin""".replace("FN_OFF", FN_OFF).replace("FN_FORMULA", FN_FORMULA))
view("rrp.insider_today", """-- today's Form 4 open-market purchases (P) and sales (S) from EDGAR's live "latest filings" feed (sec.form4_today,
-- loaded by rrp-feeds each hour at :05; added 2026-10-22 for same-day posts), one row per filing and direction, today and
-- yesterday. NOT screened (no buy_type): read footnotes and the Form 4 before posting. The nightly daily-index load
-- (rrp.insider_filings) is the record and takes over the next morning. avg_price = value / shares; value_usd is negative
-- for sales; owned_after_last = holdings after the filing's last line.
SELECT filed, accession, form, ANY_VALUE(ticker) AS ticker, ANY_VALUE(issuer) AS issuer, ANY_VALUE(issuer_cik) AS issuer_cik,
       ANY_VALUE(insider) AS insider, ANY_VALUE(role) AS role, code, MIN(tx_date) AS first_tx, MAX(tx_date) AS last_tx,
       SUM(shares) AS shares, ROUND(ABS(SAFE_DIVIDE(SUM(value), SUM(shares))), 4) AS avg_price, SUM(value) AS value_usd,
       ARRAY_AGG(owned_after IGNORE NULLS ORDER BY tx_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS owned_after_last,
       LOGICAL_OR(plan_10b5_1) AS plan, LOGICAL_OR(private_placement) AS private_placement, LOGICAL_OR(non_usd_suspect) AS non_usd,
       ANY_VALUE(footnotes) AS footnotes, MIN(loaded_at) AS first_seen,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', ANY_VALUE(issuer_cik), '/', accession, '/') AS link
FROM `P.sec.form4_today`
WHERE code IN ('P', 'S') AND filed >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
GROUP BY filed, accession, form, code""")

view("rrp.insider_filings", """-- the Form 4 screen (see rrp.insider_filings_live for every column): the nightly copy rrp-feeds saves after each Form 4
-- day loads, partitioned by filed month and clustered by ticker and code, so a read filtered on filed costs megabytes.
-- snapped_at = when the copy was made. Same rows and columns as the live view as of that time.
SELECT * FROM `P.sec.insider_filings_snap`""")

view("rrp.insider_buy_context", """-- history behind each issuer's buying day (one row per ticker per filing date with clean open-market buys, every EDGAR
-- day since 2006): the day's buyers and dollars; the previous buying day at the issuer and the last day with at least as
-- many dollars (largest_since; NULL = largest on record); buying days and non-plan sale dollars over the prior 365 days.
-- Filter on ticker. Values before 2021 come from rounded SEC data sets.
WITH b AS (SELECT ticker, filed, ANY_VALUE(issuer) AS issuer, COUNT(DISTINCT insider) AS buyers,
                  STRING_AGG(DISTINCT insider, ' / ' LIMIT 5) AS insiders, ROUND(SUM(value)) AS buy_usd, SUM(shares) AS shares
           FROM `P.rrp.insider_buys` WHERE NOT value_suspect AND NOT IFNULL(non_usd_suspect, FALSE) GROUP BY ticker, filed),
w AS (SELECT *, LAG(filed) OVER (PARTITION BY ticker ORDER BY filed) AS prev_buy_day,
             MIN(filed) OVER (PARTITION BY ticker) AS first_buy_day,
             COUNT(*) OVER (PARTITION BY ticker ORDER BY UNIX_DATE(filed) RANGE BETWEEN 365 PRECEDING AND 1 PRECEDING) AS buy_days_prior_365d
      FROM b),
big AS (SELECT a.ticker, a.filed, MAX(o.filed) AS largest_since
        FROM b a JOIN b o ON o.ticker = a.ticker AND o.filed < a.filed AND o.buy_usd >= a.buy_usd GROUP BY a.ticker, a.filed),
s AS (SELECT a.ticker, a.filed, ROUND(-SUM(t.value)) AS sale_usd_prior_365d, COUNT(DISTINCT t.insider) AS sellers_prior_365d
      FROM b a JOIN `P.rrp.insider_trades` t ON t.ticker = a.ticker AND t.code = 'S' AND NOT IFNULL(t.plan_10b5_1, FALSE)
        AND NOT t.value_suspect AND t.filed BETWEEN DATE_SUB(a.filed, INTERVAL 365 DAY) AND DATE_SUB(a.filed, INTERVAL 1 DAY)
      GROUP BY a.ticker, a.filed)
SELECT w.ticker, w.filed, w.issuer, w.buyers, w.insiders, w.buy_usd, w.shares, w.prev_buy_day, w.first_buy_day,
       big.largest_since, w.buy_days_prior_365d, IFNULL(s.sale_usd_prior_365d, 0) AS sale_usd_prior_365d, IFNULL(s.sellers_prior_365d, 0) AS sellers_prior_365d
FROM w LEFT JOIN big USING (ticker, filed) LEFT JOIN s USING (ticker, filed)""")

view("rrp.insider_followups", r"""-- what a stock did after insider buying: each issuer's clean buys filed on one day (no plan, private placement, non-dollar
-- or mismatched price, $100K+), measured 1, 5 and 20 trading sessions after the filing against the latest close.
-- pick = the order to post them in within each horizon (officers and directors first, then dollars). close_ok = the
-- latest close is the newest SPY session; implausible = more than 60% either way (check for a split or a bad print).
WITH cal AS (SELECT date, ROW_NUMBER() OVER (ORDER BY date DESC) - 1 AS n FROM `P.rrp.equity_daily`
             WHERE symbol = 'SPY' AND date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 45 DAY)),
b AS (SELECT filed, ticker, ANY_VALUE(issuer) AS issuer, STRING_AGG(DISTINCT insider, ' / ' LIMIT 3) AS insiders,
             STRING_AGG(DISTINCT role, ' / ' LIMIT 3) AS roles, COUNT(DISTINCT accession) AS filings, SUM(shares) AS total_shares,
             SUM(value) AS total_value, SAFE_DIVIDE(SUM(value), SUM(shares)) AS avg_px, MIN(tx_date) AS first_tx, MAX(tx_date) AS last_tx
      FROM `P.rrp.insider_buys_priced`
      WHERE filed >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 45 DAY)
        AND NOT IFNULL(price_mismatch, FALSE) AND NOT IFNULL(non_usd_suspect, FALSE)
        AND NOT IFNULL(private_placement, FALSE) AND NOT IFNULL(plan_10b5_1, FALSE)
      GROUP BY filed, ticker),
px AS (SELECT e.symbol, e.close, e.date FROM `P.rrp.equity_daily` e WHERE e.date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 10 DAY)
       QUALIFY ROW_NUMBER() OVER (PARTITION BY e.symbol ORDER BY e.date DESC) = 1),
j AS (SELECT cal.n AS sessions_since_filing, b.filed, b.ticker, b.issuer, b.insiders, b.roles, b.filings, ROUND(b.total_shares) AS shares,
             ROUND(b.total_value) AS value_usd, ROUND(b.avg_px, 4) AS avg_px, b.first_tx, b.last_tx, px.close AS latest_close, px.date AS close_date,
             px.date = (SELECT date FROM cal WHERE n = 0) AS close_ok,
             ROUND((px.close / b.avg_px - 1) * 100, 1) AS pct_vs_paid,
             REGEXP_CONTAINS(LOWER(b.roles), r'chief|ceo|cfo|coo|president|chair|director|officer|evp|svp|general counsel') AS officer_or_director
      FROM b JOIN cal ON b.filed = cal.date AND cal.n IN (1, 5, 20) JOIN px ON px.symbol = b.ticker
      WHERE b.total_value >= 100000)
SELECT *, ABS(pct_vs_paid) > 60 AS implausible,
       ROW_NUMBER() OVER (PARTITION BY sessions_since_filing ORDER BY officer_or_director DESC, value_usd DESC) AS pick
FROM j""")

view("rrp.edgar_completeness", r"""-- what EDGAR published vs what the warehouse loaded, per business day and filing family (b191, added 2026-10-27).
-- published = distinct filings of the family's forms in EDGAR's daily form index (ops.edgar_index_counts, written by the
-- rrp-feeds job at 08:05 and 11:05 UTC for the last 14 days); loaded = distinct accessions in the family's sec.* table
-- with that filing date. Families: Form D, 13F (HR / NT and amendments), Form 144, Schedule 13G (XML era), delistings
-- (Form 25 / 15 / 15F). Form 4 is checked per day only (sec.form4 keeps only open-market P / S lines, so its filing count
-- is a subset by design): loaded = 1 when the day has any row. status: the loaders re-read the last 5 daily indexes and
-- fetch whatever is missing, so a shortfall inside 5 business days is AMBER (still healing); older than that it is RED
-- (a permanent hole until re-run). Days are only as far back as ops.edgar_index_counts goes.
WITH fam AS (
  SELECT * FROM UNNEST([
    STRUCT('Form D' AS family, ['D', 'D/A'] AS forms),
    ('13F', ['13F-HR', '13F-HR/A', '13F-NT', '13F-NT/A']),
    ('Form 144', ['144', '144/A']),
    ('Schedule 13G', ['SCHEDULE 13G', 'SCHEDULE 13G/A']),
    ('Delistings', ['25', '25/A', '25-NSE', '25-NSE/A', '15-12B', '15-12B/A', '15-12G', '15-12G/A', '15-15D', '15-15D/A',
                    '15F-12B', '15F-12B/A', '15F-12G', '15F-12G/A', '15F-15D', '15F-15D/A']),
    ('Form 4 (day loaded)', ['4', '4/A'])])),
idx AS (SELECT day, form, filings FROM `P.ops.edgar_index_counts`),
first_day AS (SELECT MIN(day) AS d FROM idx),
pub AS (SELECT f.family, i.day, SUM(i.filings) AS published FROM idx i JOIN fam f ON i.form IN UNNEST(f.forms) GROUP BY 1, 2),
ld AS (
  SELECT 'Form D' AS family, filed AS day, COUNT(DISTINCT accession) AS loaded FROM `P.sec.formd`
    WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT '13F', filed, COUNT(DISTINCT accession) FROM `P.sec.form13f_filings` WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Form 144', filed, COUNT(DISTINCT accession) FROM `P.sec.form144` WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Schedule 13G', filed, COUNT(DISTINCT accession) FROM `P.sec.form13g` WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Delistings', filed, COUNT(DISTINCT accession) FROM `P.sec.delistings` WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Form 4 (day loaded)', filed, IF(COUNT(*) > 0, 1, 0) FROM `P.sec.form4` WHERE filed >= (SELECT d FROM first_day) GROUP BY 2),
j AS (
  SELECT p.family, p.day, p.published, IFNULL(l.loaded, 0) AS loaded,
         IF(p.family = 'Form 4 (day loaded)', IF(p.published > 0 AND IFNULL(l.loaded, 0) = 0, 1, 0),
            GREATEST(p.published - IFNULL(l.loaded, 0), 0)) AS missing,
         (SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(p.day, INTERVAL 1 DAY), CURRENT_DATE('America/New_York'))) x
          WHERE EXTRACT(DAYOFWEEK FROM x) BETWEEN 2 AND 6) AS business_days_ago
  FROM pub p LEFT JOIN ld l USING (family, day))
SELECT family, day, published, IF(family = 'Form 4 (day loaded)', NULL, loaded) AS loaded, missing, business_days_ago,
       CASE WHEN missing = 0 THEN 'GREEN' WHEN business_days_ago <= 5 THEN 'AMBER' ELSE 'RED' END AS status
FROM j""")

view("rrp.funding_extremes", """-- perpetual funding on the newest completed UTC day per coin (Kraken), measured against its own history:
-- highest_since / lowest_since = the last day funding was at least as high / as low (NULL = a record since history_from);
-- days_since_high / days_since_low; cap_rank = the coin's CoinPaprika market-cap rank (newest snapshot) for filtering.
WITH d AS (SELECT symbol, date, annualized_pct, MIN(date) OVER (PARTITION BY symbol) AS history_from
           FROM `P.rrp.funding_daily` WHERE date < CURRENT_DATE('UTC')),
l AS (SELECT * FROM d WHERE date = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)),
x AS (SELECT l.symbol, MAX(IF(d.annualized_pct >= l.annualized_pct, d.date, NULL)) AS highest_since,
             MAX(IF(d.annualized_pct <= l.annualized_pct, d.date, NULL)) AS lowest_since,
             ROUND(AVG(IF(d.date >= DATE_SUB(l.date, INTERVAL 30 DAY), d.annualized_pct, NULL)), 2) AS avg_30d_pct
      FROM l JOIN d ON d.symbol = l.symbol AND d.date < l.date GROUP BY l.symbol),
r AS (SELECT UPPER(symbol) AS symbol, MIN(rank) AS cap_rank FROM `P.rrp.crypto_market`
      WHERE ts = (SELECT MAX(ts) FROM `P.rrp.crypto_market` WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) GROUP BY 1)
SELECT l.symbol, l.date, l.annualized_pct, x.avg_30d_pct, x.highest_since, x.lowest_since, l.history_from,
       DATE_DIFF(l.date, IFNULL(x.highest_since, l.history_from), DAY) AS days_since_high,
       DATE_DIFF(l.date, IFNULL(x.lowest_since, l.history_from), DAY) AS days_since_low, r.cap_rank
FROM l JOIN x USING (symbol) LEFT JOIN r ON r.symbol = UPPER(l.symbol)""")


# ------------------------------------------------------------------ fact engine (added 2026-10-19)
view("rrp.alerts", """-- what should page the phone right now: one row per thing that is RED. Read once an hour by the rrp-markets
-- job, which logs the rows; a Cloud Monitoring log-based alert pushes RED lines to the phone, and a second alert fires
-- when the check itself stops logging. Cheap by design: run logs and free table metadata only.
-- source: an ingest source that is stale in rrp.health_feeds / rrp.health_market AND has had no good pull for 2 hours
-- (one missed hourly pull is not paged). table: a table not written within its grace window (__TABLES__ last_modified_time).
WITH rules AS (
  SELECT * FROM UNNEST([
    STRUCT('markets' AS ds, 'crypto_daily' AS tbl, 30 AS grace_h, 'daily crypto closes' AS what),
    ('markets', 'funding_rates', 30, 'funding history'),
    ('markets', 'equity_daily', 100, 'stock closes'),
    ('gov', 'cot', 200, 'CFTC COT')])),
meta AS (
  SELECT dataset_id AS ds, table_id AS tbl, TIMESTAMP_MILLIS(last_modified_time) AS lm FROM `P.markets.__TABLES__`
  UNION ALL SELECT dataset_id, table_id, TIMESTAMP_MILLIS(last_modified_time) FROM `P.gov.__TABLES__`)
SELECT 'source' AS kind, CONCAT('feeds/', source) AS item, last_ok, IFNULL(last_error, 'no good run in its window') AS detail
FROM `P.rrp.health_feeds` WHERE stale AND IFNULL(last_ok, TIMESTAMP '2000-01-01') < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
UNION ALL
SELECT 'source', CONCAT('market/', source), last_ok, IFNULL(last_error, 'no good run in its window')
FROM `P.rrp.health_market` WHERE stale AND IFNULL(last_ok, TIMESTAMP '2000-01-01') < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
UNION ALL
SELECT 'table', CONCAT(r.ds, '.', r.tbl), m.lm, CONCAT(r.what, ': not written in ', r.grace_h, 'h')
FROM rules r LEFT JOIN meta m USING (ds, tbl)
WHERE m.lm IS NULL OR m.lm < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL r.grace_h HOUR)""")

def build_views():
    wanted = {}
    for name, sql in VIEWS:
        wanted.setdefault(name.split(".")[0], set()).add(name.split(".")[1])
    print(f"project {P}: building {len(VIEWS)} views")
    for name, sql in VIEWS:
        v = bigquery.Table(f"{P}.{name}"); v.view_query = sql.replace("`P.", f"`{P}.")
        try:   # the deploy account can't read some sources (JOBS_BY_PROJECT needs bigquery.jobs.listAll): leave those views as they are
            bq.query(v.view_query, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        except Forbidden as e:
            print(f"  {name}: SKIPPED, the deploy account can't read its sources; rebuild it by hand ({str(e)[:160]})"); continue
        except BadRequest:
            pass   # e.g. a partition filter required on the whole read; the read check below handles it
        bq.delete_table(v, not_found_ok=True); bq.create_table(v)
        # a dry run checks the view compiles and shows what a full read costs, without paying for it (a real
        # COUNT(*) on every view cost ~50 GB per build on 2026-10-19 and helped trip the daily query cap)
        try:
            j = bq.query(f"SELECT * FROM `{P}.{name}`", job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
        except BadRequest as e:   # a view over a table that requires a partition filter (sec.filing_docs) can't be read whole
            if "filter over column" not in str(e) and "partition elimination" not in str(e): raise
            print(f"  {name}: ok, needs a date filter to read (its table requires one)"); continue
        print(f"  {name}: ok, full read {j.total_bytes_processed / 1e6:,.0f} MB")
    for name, args, sql in FUNCTIONS:
        bq.query(f"CREATE OR REPLACE TABLE FUNCTION `{P}.{name}`{args} AS (\n" + sql.replace("`P.", f"`{P}.") + "\n)").result()
        print(f"  {name}{args}: table function ok")
    for ds, names in wanted.items():
        for t in bq.list_tables(f"{P}.{ds}"):
            if t.table_type == "VIEW" and t.table_id not in names:
                bq.delete_table(f"{P}.{ds}.{t.table_id}"); print(f"  deleted {ds}.{t.table_id} (no longer defined)")


def meta_for(ds, table):
    key = f"{ds}.{table}"
    if key in CATALOG: return CATALOG[key]
    for k in sorted((k for k in CATALOG if k.endswith("*")), key=len, reverse=True):
        if key.startswith(k[:-1]): return CATALOG[k]
    return None


def write_dictionary():
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = ["# Warehouse data dictionary", "",
             f"Project `{P}`. Generated by `warehouse/build.py` on {now} UTC. Do not edit by hand: edit `warehouse/catalog.py` "
             "(descriptions) or `warehouse/build.py` (views) and rerun it.", "",
             "Cloud jobs load raw data into `sec`, `gov` and `markets`; the `rrp` views are the reading layer. Start with `rrp`.", ""]
    missing = []
    for ds, ds_desc in DATASETS.items():
        try: d = bq.get_dataset(f"{P}.{ds}")
        except NotFound: continue
        if d.description != ds_desc:
            d.description = ds_desc
            try: bq.update_dataset(d, ["description"])
            except Forbidden:   # the deploy account has no bigquery.datasets.update: the dictionary still uses the catalog text
                print(f"  {ds}: dataset description not updated (deploy account lacks datasets.update)")
        lines += [f"## `{ds}`", "", ds_desc, ""]
        if ds == "staging":   # scratch tables come and go
            lines.append(""); continue
        for t in sorted(bq.list_tables(f"{P}.{ds}"), key=lambda x: x.table_id):
            tb = bq.get_table(f"{P}.{ds}.{t.table_id}")
            meta = meta_for(ds, t.table_id)
            if not meta:
                missing.append(f"{ds}.{t.table_id}")
                meta = {"what": tb.description or "UNDOCUMENTED - add to catalog.py"}
            else:
                desc = f"{meta['what']} | Source: {meta['source']} | Updates: {meta['cadence']} | One row = {meta['grain']}"
                if tb.description != desc:
                    tb.description = desc; bq.update_table(tb, ["description"])
            lines += [f"### `{ds}.{t.table_id}` ({tb.table_type.lower()})", "", f"- **What:** {meta['what']}"]
            for label, k in (("Source", "source"), ("Updates", "cadence"), ("One row =", "grain"), ("Notes", "notes")):
                if meta.get(k):
                    lines.append(f"- **{label}** {meta[k]}" if label.endswith("=") else f"- **{label}:** {meta[k]}")
            lines += ["", "| column | type |", "|---|---|"] + [f"| `{f.name}` | {f.field_type} |" for f in tb.schema] + [""]
        lines.append("")
    open(os.path.join(HERE, "..", "DATA_DICTIONARY.md"), "w", encoding="utf-8").write("\n".join(lines))
    print(f"DATA_DICTIONARY.md written ({len(lines)} lines); not in catalog.py: {', '.join(missing) or 'none'}")


if __name__ == "__main__":
    build_views()
    write_dictionary()
