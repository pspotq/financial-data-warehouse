-- Crypto Fear & Greed: newest day, its classification, how many consecutive days it has held that classification,
-- and the reading a week earlier
WITH f AS (SELECT *, LAG(value, 7) OVER (ORDER BY date) AS value_7d_ago,
                  COUNTIF(classification != prev OR prev IS NULL) OVER (ORDER BY date) AS grp
           FROM (SELECT *, LAG(classification) OVER (ORDER BY date) AS prev FROM {{ ref('crypto_fear_greed') }}))
SELECT date, value, classification, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY date) AS days_in_classification, value_7d_ago
FROM f QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1
