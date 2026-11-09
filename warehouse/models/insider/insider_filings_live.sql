-- the Form 4 screen, computed live (~3.2 GB a read; tasks read rrp.insider_filings, the nightly copy): one row per filing and direction (P = buys, S = sales), every EDGAR day since 2006. Filter on filed.
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
  FROM {{ ref('insider_trades') }} WHERE form IN ('4', '4/A') AND code IN ('P', 'S')),
t AS (SELECT * FROM raw WHERE NOT (joint AND rn > 1)),
clean AS (SELECT DISTINCT filed, accession FROM {{ ref('insider_buys') }}),
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
    REGEXP_CONTAINS(f.fn, r'privately negotiated|private transaction|private sale|private placement|purchase agreement|sale agreement|subscription agreement|were issued|issued pursuant|issued to the reporting|public offering|underwrit|secondary offering|upon conversion|conversion of|tendered|tender offer|offer to purchase|repurchase|redeem|redemption|merger|rebalanc|no shares were bought or sold|in-kind|in kind|pro rata|distribution|exchange agreement|exchange fund|block trade|gift|cash settlement') AS off_market,
    REGEXP_CONTAINS(f.fn, r'average of the high and low|average of the closing|average closing|closing price|closing sale price|fair market value|net asset value') AS formula_price,
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
    REGEXP_EXTRACT(f.fn, r'.{0,90}(?:privately negotiated|private transaction|private sale|private placement|purchase agreement|sale agreement|subscription agreement|were issued|issued pursuant|issued to the reporting|public offering|underwrit|secondary offering|upon conversion|conversion of|tendered|tender offer|offer to purchase|repurchase|redeem|redemption|merger|rebalanc|no shares were bought or sold|in-kind|in kind|pro rata|distribution|exchange agreement|exchange fund|block trade|gift|cash settlement|average of the high and low|average of the closing|average closing|closing price|closing sale price|fair market value|net asset value).{0,110}') AS flag_context,
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
       FROM {{ ref('insider_buys_priced') }} GROUP BY accession),
sz AS (SELECT accession, SUM(buy_pct_of_mkt_cap) AS pct_mkt_cap FROM {{ ref('insider_buys_sized') }} GROUP BY accession),
blk AS (SELECT DISTINCT b.accession FROM t b JOIN (SELECT DISTINCT ticker, tx_date, shares, price FROM t WHERE code = 'S') s
          ON s.ticker = b.ticker AND s.tx_date = b.tx_date AND s.shares = b.shares AND s.price = b.price WHERE b.code = 'P'),
fxr AS (SELECT CASE label WHEN 'USDBRL' THEN 'BRL' WHEN 'USDMXN' THEN 'MXN' WHEN 'USDHKD' THEN 'HKD' WHEN 'USDCAD' THEN 'CAD'
                          WHEN 'USDJPY' THEN 'JPY' WHEN 'EURUSD' THEN 'EUR' END AS ccy,
               obs_date, IF(label = 'EURUSD', 1 / close, close) AS per_usd
        FROM {{ ref('macro_daily') }} WHERE source = 'fred' AND close > 0
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
FROM fin
