-- what should page the phone right now: one row per thing that is RED. Read once an hour by the rrp-markets
-- job, which logs the rows; a Cloud Monitoring log-based alert pushes RED lines to the phone, and a second alert fires
-- when the check itself stops logging. Cheap by design: run logs and free table metadata only.
-- source: an ingest source that is stale in rrp.health_feeds / rrp.health_market AND has had no good pull for 2 hours
-- (one missed hourly pull is not paged). table: a table not written within its grace window (__TABLES__ last_modified_time).
WITH rules AS (
  SELECT * FROM UNNEST([
    STRUCT('markets' AS ds, 'crypto_daily' AS tbl, 30 AS grace_h, 'daily crypto closes' AS what),
    ('markets', 'funding_rates', 30, 'funding history'),
    ('markets', 'equity_daily', 100, 'stock closes'),
    ('gov', 'cot', 200, 'CFTC COT')])),
meta AS (
  SELECT dataset_id AS ds, table_id AS tbl, TIMESTAMP_MILLIS(last_modified_time) AS lm FROM {{ source('markets', '__TABLES__') }}
  UNION ALL SELECT dataset_id, table_id, TIMESTAMP_MILLIS(last_modified_time) FROM {{ source('gov', '__TABLES__') }})
SELECT 'source' AS kind, CONCAT('feeds/', source) AS item, last_ok, IFNULL(last_error, 'no good run in its window') AS detail
FROM {{ ref('health_feeds') }} WHERE stale AND IFNULL(last_ok, TIMESTAMP '2000-01-01') < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
UNION ALL
SELECT 'source', CONCAT('market/', source), last_ok, IFNULL(last_error, 'no good run in its window')
FROM {{ ref('health_market') }} WHERE stale AND IFNULL(last_ok, TIMESTAMP '2000-01-01') < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
UNION ALL
SELECT 'table', CONCAT(r.ds, '.', r.tbl), m.lm, CONCAT(r.what, ': not written in ', r.grace_h, 'h')
FROM rules r LEFT JOIN meta m USING (ds, tbl)
WHERE m.lm IS NULL OR m.lm < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL r.grace_h HOUR)
