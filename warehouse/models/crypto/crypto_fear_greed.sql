-- Crypto Fear & Greed index (alternative.me), one row per day
SELECT date, value, classification FROM {{ source('markets', 'crypto_fear_greed') }}
