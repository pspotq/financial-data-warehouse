-- tickers the daily equities pass must cover, with why each one is on the list. priority orders first-time backfills
WITH core AS (
  SELECT t AS ticker, 'core' AS reason, 1 AS priority FROM UNNEST([
    'SPY','QQQ','IWM','DIA','TLT','HYG','LQD','GLD','XLE','XLF','XLK','XLU','ITA','KRE',
    'MSTR','COIN','MARA','RIOT','CLSK','HUT','CORZ','HOOD','IBIT','FBTC','ETHA','GLXY','CRCL','BMNR','SBET',
    'LMT','RTX','NOC','GD','HII','LHX','BA','LDOS','BAH','KTOS','SAIC','CACI','TPC','PSN','AVAV','KRMN']) t),
funds AS (
  SELECT t AS ticker, 'fund_universe' AS reason, 1 AS priority FROM UNNEST([
    'FXAIX','FSPSX','SWSSX','FSMDX','FSSNX','FPADX','FBGRX','MINDX',
    'FSUTX','FSPTX','FSDAX','FSENX','FSAGX','FIDSX','FSMEX','FSELX','FSCSX','FBIOX','FSPHX','FPHAX',
    'FSHCX','FSRPX','FSRFX','FSCHX','FSDPX','FSHOX','FSLBX','FSRBX','FSPCX','FSCPX','FDFAX','FSTCX',
    'FSAIX','FSAVX','FSLEX','FNARX','FCYIX','FWRLX','FSDCX','FBMPX','FSESX',
    'FXNAX','FUMBX','FIPDX']) t),
ins AS (
  SELECT ticker, 'insider_buy' AS reason, 2 AS priority FROM {{ ref('insider_buys') }}
  WHERE tx_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 180 DAY)
  GROUP BY ticker HAVING SUM(value) >= 100000),
fh AS (
  SELECT DISTINCT REGEXP_EXTRACT(filer, r'\(([A-Z]{1,5}(?:-[A-Z]{1,2})?)[,)]') AS ticker, CONCAT('filing:', stream) AS reason,
         IF(stream IN ('merger_agreement', 'tender_third_party', 'tender_issuer', 'activist_13d'), 3, 4) AS priority
  FROM {{ ref('sec_filings') }} WHERE file_date >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 90 DAY))
SELECT ticker, MIN(priority) AS priority, STRING_AGG(DISTINCT reason, ', ' ORDER BY reason) AS reasons
FROM (SELECT * FROM core UNION ALL SELECT * FROM funds UNION ALL SELECT * FROM ins UNION ALL SELECT * FROM fh)
WHERE ticker IS NOT NULL AND ticker NOT IN ('NONE', 'NA')
GROUP BY ticker
