-- filings first seen in the last 26 hours AND filed in the last 3 days: the working list for the morning and sweep runs
SELECT * FROM {{ ref('sec_filings') }}
WHERE first_seen > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 26 HOUR)
  AND file_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 DAY)
