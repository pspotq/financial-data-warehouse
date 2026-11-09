-- company facts from 10-K / 10-Q / 20-F filings (SEC XBRL): one row per company, concept, unit, period and filing.
-- filed is the filing date (NULL on rows from the weekly frames refresh); use only rows filed before the date you study.
-- Rows with an impossible period end (before EDGAR, or more than 400 days after filing, e.g. 2215) are left out: ~570 of 14.7M.
SELECT cik, entity, taxonomy, concept, unit, period_start, period_end, val, accn, fy, fp, form, filed, frame, source
FROM {{ source('sec', 'xbrl_facts') }}
WHERE period_end BETWEEN DATE '1993-01-01' AND DATE_ADD(COALESCE(filed, CURRENT_DATE()), INTERVAL 400 DAY)
