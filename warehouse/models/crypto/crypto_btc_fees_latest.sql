-- bitcoin network fee estimates (mempool.space, sat/vB), latest reading
SELECT * FROM {{ source('markets', 'btc_fees') }}
WHERE ts = (SELECT MAX(ts) FROM {{ source('markets', 'btc_fees') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
