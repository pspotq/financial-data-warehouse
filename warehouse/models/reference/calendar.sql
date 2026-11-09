-- one row per day, 2006 to 30 days ahead: whether US stock markets traded (SPY has a bar) and whether EDGAR posted
-- filings (sec.form4 has rows), and the previous such day. Days the data has not reached yet (today and later) use the
-- published 2026-2027 NYSE and federal holiday lists; *_confirmed = TRUE when the day is backed by data.
WITH spy AS (SELECT DISTINCT date FROM {{ ref('equity_daily') }} WHERE symbol = 'SPY'),
ed AS (SELECT DISTINCT filed AS date FROM {{ source('sec', 'form4') }}),
known AS (SELECT MAX(date) AS spy_max FROM spy),
k2 AS (SELECT MAX(date) AS ed_max FROM ed),
days AS (SELECT d AS date FROM UNNEST(GENERATE_DATE_ARRAY('2006-01-01', DATE_ADD(CURRENT_DATE('America/New_York'), INTERVAL 30 DAY))) d),
x AS (SELECT days.date, EXTRACT(DAYOFWEEK FROM days.date) BETWEEN 2 AND 6 AS weekday,
        CASE WHEN days.date <= (SELECT spy_max FROM known) THEN spy.date IS NOT NULL END AS nyse_known,
        CASE WHEN days.date <= (SELECT ed_max FROM k2) THEN ed.date IS NOT NULL END AS edgar_known
      FROM days LEFT JOIN spy USING (date) LEFT JOIN ed USING (date)),
y AS (SELECT *, COALESCE(nyse_known, weekday AND date NOT IN UNNEST([DATE '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03',
          '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25', '2027-01-01', '2027-01-18',
          '2027-02-15', '2027-03-26', '2027-05-31', '2027-06-18', '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24'])) AS nyse_open,
        COALESCE(edgar_known, weekday AND date NOT IN UNNEST([DATE '2026-01-01', '2026-01-19', '2026-02-16', '2026-05-25',
          '2026-06-19', '2026-07-03', '2026-09-07', '2026-10-12', '2026-11-11', '2026-11-26', '2026-12-25', '2027-01-01',
          '2027-01-18', '2027-02-15', '2027-05-31', '2027-06-18', '2027-07-05', '2027-09-06', '2027-10-11', '2027-11-11',
          '2027-11-25', '2027-12-24'])) AS edgar_open
      FROM x)
SELECT date, FORMAT_DATE('%a', date) AS dow, weekday, nyse_open, nyse_known IS NOT NULL AS nyse_confirmed,
       edgar_open, edgar_known IS NOT NULL AS edgar_confirmed,
       LAST_VALUE(IF(nyse_open, date, NULL) IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_nyse_day,
       LAST_VALUE(IF(edgar_open, date, NULL) IGNORE NULLS) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_edgar_day
FROM y
