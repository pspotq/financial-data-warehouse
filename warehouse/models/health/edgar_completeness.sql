-- what EDGAR published vs what the warehouse loaded, per business day and filing family (b191, added 2026-10-27).
-- published = distinct filings of the family's forms in EDGAR's daily form index (ops.edgar_index_counts, written by the
-- rrp-feeds job at 08:05 and 11:05 UTC for the last 14 days); loaded = distinct accessions in the family's sec.* table
-- with that filing date. Families: Form D, 13F (HR / NT and amendments), Form 144, Schedule 13G (XML era), delistings
-- (Form 25 / 15 / 15F). Form 4 is checked per day only (sec.form4 keeps only open-market P / S lines, so its filing count
-- is a subset by design): loaded = 1 when the day has any row. status: the loaders re-read the last 5 daily indexes and
-- fetch whatever is missing, so a shortfall inside 5 business days is AMBER (still healing); older than that it is RED
-- (a permanent hole until re-run). Days are only as far back as ops.edgar_index_counts goes.
WITH fam AS (
  SELECT * FROM UNNEST([
    STRUCT('Form D' AS family, ['D', 'D/A'] AS forms),
    ('13F', ['13F-HR', '13F-HR/A', '13F-NT', '13F-NT/A']),
    ('Form 144', ['144', '144/A']),
    ('Schedule 13G', ['SCHEDULE 13G', 'SCHEDULE 13G/A']),
    ('Delistings', ['25', '25/A', '25-NSE', '25-NSE/A', '15-12B', '15-12B/A', '15-12G', '15-12G/A', '15-15D', '15-15D/A',
                    '15F-12B', '15F-12B/A', '15F-12G', '15F-12G/A', '15F-15D', '15F-15D/A']),
    ('Form 4 (day loaded)', ['4', '4/A'])])),
idx AS (SELECT day, form, filings FROM {{ source('ops', 'edgar_index_counts') }}),
first_day AS (SELECT MIN(day) AS d FROM idx),
pub AS (SELECT f.family, i.day, SUM(i.filings) AS published FROM idx i JOIN fam f ON i.form IN UNNEST(f.forms) GROUP BY 1, 2),
ld AS (
  SELECT 'Form D' AS family, filed AS day, COUNT(DISTINCT accession) AS loaded FROM {{ source('sec', 'formd') }}
    WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT '13F', filed, COUNT(DISTINCT accession) FROM {{ source('sec', 'form13f_filings') }} WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Form 144', filed, COUNT(DISTINCT accession) FROM {{ source('sec', 'form144') }} WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Schedule 13G', filed, COUNT(DISTINCT accession) FROM {{ source('sec', 'form13g') }} WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Delistings', filed, COUNT(DISTINCT accession) FROM {{ source('sec', 'delistings') }} WHERE filed >= (SELECT d FROM first_day) GROUP BY 2
  UNION ALL SELECT 'Form 4 (day loaded)', filed, IF(COUNT(*) > 0, 1, 0) FROM {{ source('sec', 'form4') }} WHERE filed >= (SELECT d FROM first_day) GROUP BY 2),
j AS (
  SELECT p.family, p.day, p.published, IFNULL(l.loaded, 0) AS loaded,
         IF(p.family = 'Form 4 (day loaded)', IF(p.published > 0 AND IFNULL(l.loaded, 0) = 0, 1, 0),
            GREATEST(p.published - IFNULL(l.loaded, 0), 0)) AS missing,
         (SELECT COUNT(*) FROM UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(p.day, INTERVAL 1 DAY), CURRENT_DATE('America/New_York'))) x
          WHERE EXTRACT(DAYOFWEEK FROM x) BETWEEN 2 AND 6) AS business_days_ago
  FROM pub p LEFT JOIN ld l USING (family, day))
SELECT family, day, published, IF(family = 'Form 4 (day loaded)', NULL, loaded) AS loaded, missing, business_days_ago,
       CASE WHEN missing = 0 THEN 'GREEN' WHEN business_days_ago <= 5 THEN 'AMBER' ELSE 'RED' END AS status
FROM j
