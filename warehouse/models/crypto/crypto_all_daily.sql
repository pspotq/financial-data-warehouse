-- daily OHLCV for every Kraken and Coinbase USD pair (full history each venue serves). base = the coin; tickers can
-- collide across projects, so check the price against rrp.crypto_market before treating two rows as one coin. is_open = today
-- Bad bars are left out: a date before bitcoin's first block (Kraken sent a 1970-01-01 PIPEUSD bar) or a price of 0 or less.
SELECT venue, pair, base, quote, date, open, high, low, close, vwap, volume, trades, is_open
FROM {{ source('markets', 'crypto_daily') }} WHERE date >= '2009-01-03' AND close > 0 AND open > 0 AND low > 0 AND high > 0
