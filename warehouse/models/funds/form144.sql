-- every Form 144 / 144/A (notice of proposed sale of restricted or control stock by an insider or affiliate, filed
-- on or before the sale), 2025 on, one row per security line. One filing can list several lines. ticker = the issuer's
-- ticker on the filing date (rrp.entity_tickers). Amounts in US$ as the filer typed them: implied_price =
-- market_value / units_to_sell, and value_suspect marks lines whose implied price is over $10,000 a share (a typo in
-- the filing, e.g. cents typed as dollars); leave those out of sums. pct_of_outstanding = units_to_sell /
-- units_outstanding x 100. under_10b5_1 = the seller named a Rule 10b5-1 trading plan (plan_adoption_date). A 144
-- is an intention, not a trade: the Form 4 that follows (rrp.insider_*) reports the actual sale.
WITH t AS (SELECT cik, ticker, valid_from, valid_to, form4_filings FROM {{ ref('entity_tickers') }})
SELECT f.accession, f.filed, f.form, f.form = '144/A' AS is_amendment, f.line, f.issuer_cik, f.issuer_name, t.ticker,
       f.seller, f.relationship, f.security_class, f.broker, f.units_to_sell, f.market_value,
       ROUND(SAFE_DIVIDE(f.market_value, f.units_to_sell), 4) AS implied_price,
       IFNULL(SAFE_DIVIDE(f.market_value, f.units_to_sell) > 10000 AND f.issuer_cik != '1067983', FALSE) AS value_suspect,
       f.units_outstanding, ROUND(100 * SAFE_DIVIDE(f.units_to_sell, f.units_outstanding), 4) AS pct_of_outstanding,
       f.approx_sale_date, f.exchange, f.acquired_date, f.acquisition_nature, f.sold_past_3m_units, f.sold_past_3m_value,
       f.plan_adoption_date, f.plan_adoption_date IS NOT NULL AS under_10b5_1, f.notice_date,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', f.issuer_cik, '/', f.accession, '/') AS url
FROM {{ source('sec', 'form144') }} f
LEFT JOIN t ON t.cik = f.issuer_cik AND f.filed BETWEEN t.valid_from AND IFNULL(t.valid_to, DATE '9999-12-31')
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY f.accession, f.line ORDER BY t.form4_filings DESC, f.ingested_at DESC) = 1
