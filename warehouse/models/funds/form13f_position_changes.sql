-- what each 13F manager bought and sold in its newest quarter (b212, 2026-10-26): for the latest quarter end and the one
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
  FROM {{ ref('form13f_holdings') }}
  WHERE period IN (DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY),   -- written out (not read from qq) so
                   DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER), INTERVAL 1 DAY))   -- BigQuery prunes partitions
    AND put_call IS NULL AND shares_type = 'SH'
  GROUP BY period, cik, cusip),
filed_cur AS (SELECT DISTINCT cik FROM pos WHERE period = (SELECT cur FROM qq)),
tot AS (SELECT cik, period, value_usd AS manager_value_usd FROM {{ ref('form13f_managers') }}
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
LEFT JOIN {{ ref('cusip_map') }} m ON m.cusip8 = SUBSTR(j.cusip, 1, 8)
WHERE GREATEST(j.value_usd, j.prev_value_usd) >= 25e6
