-- the 12 core coins (Kraken): hourly candles; is_open = the current unfinished hour
SELECT symbol, bar_start, open, high, low, close, vwap, volume, trades, is_open
FROM {{ source('markets', 'crypto_ohlc') }} WHERE interval_min = 60
