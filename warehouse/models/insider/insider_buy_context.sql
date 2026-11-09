-- history behind each issuer's buying day (one row per ticker per filing date with clean open-market buys, every EDGAR
-- day since 2006): the day's buyers and dollars; the previous buying day at the issuer and the last day with at least as
-- many dollars (largest_since; NULL = largest on record); buying days and non-plan sale dollars over the prior 365 days.
-- Filter on ticker. Values before 2021 come from rounded SEC data sets.
WITH b AS (SELECT ticker, filed, ANY_VALUE(issuer) AS issuer, COUNT(DISTINCT insider) AS buyers,
                  STRING_AGG(DISTINCT insider, ' / ' LIMIT 5) AS insiders, ROUND(SUM(value)) AS buy_usd, SUM(shares) AS shares
           FROM {{ ref('insider_buys') }} WHERE NOT value_suspect AND NOT IFNULL(non_usd_suspect, FALSE) GROUP BY ticker, filed),
w AS (SELECT *, LAG(filed) OVER (PARTITION BY ticker ORDER BY filed) AS prev_buy_day,
             MIN(filed) OVER (PARTITION BY ticker) AS first_buy_day,
             COUNT(*) OVER (PARTITION BY ticker ORDER BY UNIX_DATE(filed) RANGE BETWEEN 365 PRECEDING AND 1 PRECEDING) AS buy_days_prior_365d
      FROM b),
big AS (SELECT a.ticker, a.filed, MAX(o.filed) AS largest_since
        FROM b a JOIN b o ON o.ticker = a.ticker AND o.filed < a.filed AND o.buy_usd >= a.buy_usd GROUP BY a.ticker, a.filed),
s AS (SELECT a.ticker, a.filed, ROUND(-SUM(t.value)) AS sale_usd_prior_365d, COUNT(DISTINCT t.insider) AS sellers_prior_365d
      FROM b a JOIN {{ ref('insider_trades') }} t ON t.ticker = a.ticker AND t.code = 'S' AND NOT IFNULL(t.plan_10b5_1, FALSE)
        AND NOT t.value_suspect AND t.filed BETWEEN DATE_SUB(a.filed, INTERVAL 365 DAY) AND DATE_SUB(a.filed, INTERVAL 1 DAY)
      GROUP BY a.ticker, a.filed)
SELECT w.ticker, w.filed, w.issuer, w.buyers, w.insiders, w.buy_usd, w.shares, w.prev_buy_day, w.first_buy_day,
       big.largest_since, w.buy_days_prior_365d, IFNULL(s.sale_usd_prior_365d, 0) AS sale_usd_prior_365d, IFNULL(s.sellers_prior_365d, 0) AS sellers_prior_365d
FROM w LEFT JOIN big USING (ticker, filed) LEFT JOIN s USING (ticker, filed)
