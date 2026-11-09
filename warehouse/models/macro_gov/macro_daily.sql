-- every benchmark close by market date and source (rates, indexes, VIX, commodities, FX), history for look-backs.
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
FROM {{ source('markets', 'macro') }}
