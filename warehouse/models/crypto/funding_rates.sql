-- perpetual futures funding, every funding period: Kraken Futures (PF_ perpetuals and the old PI_ BTC/ETH inverse
-- perps; Kraken serves about a year) and Deribit BTC-PERPETUAL / ETH-PERPETUAL (hourly, back to 2019). rate_pct = the period's rate as % of notional (positive = longs pay shorts);
-- annualized_pct scales it by the instrument's own spacing between funding periods.
WITH r AS (SELECT *, TIMESTAMP_DIFF(ts, LAG(ts) OVER (PARTITION BY venue, instrument ORDER BY ts), SECOND) AS gap_s
           FROM {{ source('markets', 'funding_rates') }})
SELECT venue, instrument, symbol, ts, ROUND(100 * relative_funding_rate, 6) AS rate_pct,
       ROUND(100 * relative_funding_rate * 31536000 / NULLIF(gap_s, 0), 3) AS annualized_pct, funding_rate
FROM r
