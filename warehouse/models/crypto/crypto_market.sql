-- CoinPaprika top 2,000 coins, one snapshot per hour (ts): price, volume, market cap, % changes, ATH and its date,
-- first_data_at (project age). Filter on ts to read one snapshot
SELECT * FROM {{ source('markets', 'crypto_market') }}
