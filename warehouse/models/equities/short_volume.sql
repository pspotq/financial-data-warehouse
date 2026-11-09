-- FINRA Reg SHO daily short sale volume (consolidated NMS file), every trading day from Aug 2018, one row per symbol.
-- CAUTION: total_volume is only the volume reported to FINRA's trade reporting facilities (mostly off-exchange), NOT the
-- stock's full consolidated volume, and short_volume counts sales marked short, including market makers' routine
-- hedging. short_pct near 40-50% is normal; it is not short interest. Compare a stock against its own history
-- (short_pct_20d_avg), never across stocks. Published each trading evening; loaded by the weekday 22:20 UTC pass.
SELECT date, symbol, short_volume, short_exempt_volume, total_volume, market,
       ROUND(100 * SAFE_DIVIDE(short_volume, total_volume), 2) AS short_pct,
       ROUND(100 * SAFE_DIVIDE(SUM(short_volume) OVER w, SUM(total_volume) OVER w), 2) AS short_pct_20d_avg
FROM {{ source('markets', 'short_volume') }}
WINDOW w AS (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
