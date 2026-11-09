-- daily bars per ticker (Yahoo chart API). adj_close is split/dividend adjusted: use it for returns
SELECT symbol, date, open, high, low, close, adj_close, volume, currency, exchange, instrument_type
FROM {{ source('markets', 'equity_daily') }}
