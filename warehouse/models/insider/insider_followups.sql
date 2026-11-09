-- what a stock did after insider buying: each issuer's clean buys filed on one day (no plan, private placement, non-dollar
-- or mismatched price, $100K+), measured 1, 5 and 20 trading sessions after the filing against the latest close.
-- pick = the order to post them in within each horizon (officers and directors first, then dollars). close_ok = the
-- latest close is the newest SPY session; implausible = more than 60% either way (check for a split or a bad print).
WITH cal AS (SELECT date, ROW_NUMBER() OVER (ORDER BY date DESC) - 1 AS n FROM {{ ref('equity_daily') }}
             WHERE symbol = 'SPY' AND date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 45 DAY)),
b AS (SELECT filed, ticker, ANY_VALUE(issuer) AS issuer, STRING_AGG(DISTINCT insider, ' / ' LIMIT 3) AS insiders,
             STRING_AGG(DISTINCT role, ' / ' LIMIT 3) AS roles, COUNT(DISTINCT accession) AS filings, SUM(shares) AS total_shares,
             SUM(value) AS total_value, SAFE_DIVIDE(SUM(value), SUM(shares)) AS avg_px, MIN(tx_date) AS first_tx, MAX(tx_date) AS last_tx
      FROM {{ ref('insider_buys_priced') }}
      WHERE filed >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 45 DAY)
        AND NOT IFNULL(price_mismatch, FALSE) AND NOT IFNULL(non_usd_suspect, FALSE)
        AND NOT IFNULL(private_placement, FALSE) AND NOT IFNULL(plan_10b5_1, FALSE)
      GROUP BY filed, ticker),
px AS (SELECT e.symbol, e.close, e.date FROM {{ ref('equity_daily') }} e WHERE e.date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 10 DAY)
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
FROM j
