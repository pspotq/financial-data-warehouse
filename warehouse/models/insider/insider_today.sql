-- today's Form 4 open-market purchases (P) and sales (S) from EDGAR's live "latest filings" feed (sec.form4_today,
-- loaded by rrp-feeds each hour at :05; added 2026-10-22 for same-day posts), one row per filing and direction, today and
-- yesterday. NOT screened (no buy_type): read footnotes and the Form 4 before posting. The nightly daily-index load
-- (rrp.insider_filings) is the record and takes over the next morning. avg_price = value / shares; value_usd is negative
-- for sales; owned_after_last = holdings after the filing's last line.
SELECT filed, accession, form, ANY_VALUE(ticker) AS ticker, ANY_VALUE(issuer) AS issuer, ANY_VALUE(issuer_cik) AS issuer_cik,
       ANY_VALUE(insider) AS insider, ANY_VALUE(role) AS role, code, MIN(tx_date) AS first_tx, MAX(tx_date) AS last_tx,
       SUM(shares) AS shares, ROUND(ABS(SAFE_DIVIDE(SUM(value), SUM(shares))), 4) AS avg_price, SUM(value) AS value_usd,
       ARRAY_AGG(owned_after IGNORE NULLS ORDER BY tx_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS owned_after_last,
       LOGICAL_OR(plan_10b5_1) AS plan, LOGICAL_OR(private_placement) AS private_placement, LOGICAL_OR(non_usd_suspect) AS non_usd,
       ANY_VALUE(footnotes) AS footnotes, MIN(loaded_at) AS first_seen,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', ANY_VALUE(issuer_cik), '/', accession, '/') AS link
FROM {{ source('sec', 'form4_today') }}
WHERE code IN ('P', 'S') AND filed >= DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 1 DAY)
GROUP BY filed, accession, form, code
