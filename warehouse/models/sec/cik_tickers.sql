-- every ticker the SEC has mapped to a company since the daily snapshots began, with first and last day seen;
-- is_current = on the newest list (a delisted company drops off the SEC list but stays here)
WITH t AS (SELECT cik, ticker, ANY_VALUE(name) AS name, ANY_VALUE(exchange) AS exchange, MIN(as_of) AS first_seen, MAX(as_of) AS last_seen
           FROM {{ source('sec', 'cik_tickers') }} GROUP BY cik, ticker)
SELECT *, last_seen = (SELECT MAX(as_of) FROM {{ source('sec', 'cik_tickers') }}) AS is_current FROM t
