-- the 12 core coins (Kraken): completed daily candles with MA20 / MA50 / RSI14 (Wilder)
SELECT o.symbol, DATE(o.bar_start) AS date, o.open, o.high, o.low, o.close, o.vwap, o.volume, o.trades,
       i.ma20, i.ma50, i.rsi14
FROM {{ source('markets', 'crypto_ohlc') }} o LEFT JOIN {{ source('markets', 'crypto_indicators_daily') }} i ON i.symbol = o.symbol AND i.date = DATE(o.bar_start)
WHERE o.interval_min = 1440 AND NOT o.is_open
