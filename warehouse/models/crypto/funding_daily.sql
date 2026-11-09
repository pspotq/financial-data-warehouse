-- one row per coin per UTC day: average annualized funding from one source per day, in order of preference:
-- the coin's Kraken PF_ perpetuals, else Kraken's old PI_ inverse perp, else Deribit (BTC and ETH before Kraken's year)
WITH r AS (SELECT *, CASE WHEN STARTS_WITH(instrument, 'PF_') THEN 1 WHEN venue = 'deribit' THEN 3 ELSE 2 END AS pref
           FROM {{ ref('funding_rates') }} WHERE annualized_pct IS NOT NULL),
k AS (SELECT *, MIN(pref) OVER (PARTITION BY symbol, DATE(ts)) AS best FROM r)
SELECT symbol, DATE(ts) AS date, ROUND(AVG(annualized_pct), 3) AS annualized_pct, COUNT(*) AS periods,
       ARRAY_AGG(DISTINCT instrument) AS instruments
FROM k WHERE pref = best
GROUP BY symbol, date
