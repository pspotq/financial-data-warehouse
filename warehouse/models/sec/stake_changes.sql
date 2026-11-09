-- every 13D filing with the previous filing's stake for the same issuer (CUSIP, else name) and lead filer:
-- prev_pct, pct_change, first_filing (no earlier one), crossed_up / crossed_down 10% and 20%, exited (pct now 0)
WITH s AS (SELECT acc18, accession, form, file_date, event_date, issuer, cusip, lead_filer, top_filer, filers, pct, shares,
                  funds, filer_types, purpose_flags, amendment_no,
                  COALESCE(cusip, UPPER(REGEXP_REPLACE(issuer, r'[^A-Za-z0-9]', ''))) AS issuer_key,
                  UPPER(REGEXP_REPLACE(lead_filer, r'[^A-Za-z0-9]', '')) AS filer_key
           FROM {{ ref('stakes') }} WHERE pct IS NOT NULL),
l AS (SELECT *, LAG(pct) OVER w AS prev_pct, LAG(file_date) OVER w AS prev_date, LAG(shares) OVER w AS prev_shares
      FROM s WINDOW w AS (PARTITION BY issuer_key, filer_key ORDER BY file_date, acc18))
SELECT * EXCEPT (issuer_key, filer_key), ROUND(pct - prev_pct, 2) AS pct_change, prev_pct IS NULL AS first_filing,
       (prev_pct < 10 AND pct >= 10) OR (prev_pct < 20 AND pct >= 20) AS crossed_up,
       (prev_pct >= 10 AND pct < 10) OR (prev_pct >= 20 AND pct < 20) AS crossed_down,
       pct = 0 AND IFNULL(prev_pct, 0) > 0 AS exited
FROM l
