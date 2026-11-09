-- every stablecoin DefiLlama tracks, latest hourly reading: circulating supply now, a day, a week and a month ago
SELECT * FROM {{ source('markets', 'stablecoins') }}
WHERE ts = (SELECT MAX(ts) FROM {{ source('markets', 'stablecoins') }} WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY))
