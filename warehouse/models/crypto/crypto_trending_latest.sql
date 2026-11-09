-- Coinbase trending list, latest snapshot
SELECT * FROM {{ source('markets', 'crypto_trending') }}
WHERE ts = (SELECT MAX(ts) FROM {{ source('markets', 'crypto_trending') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
