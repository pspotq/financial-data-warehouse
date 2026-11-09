-- every SEC investment adviser, one row per firm (crd = its FINRA CRD number, the stable id) per monthly SEC snapshot,
-- Feb 2019 on (Oct / Nov 2025 and Jan 2019 missing: SEC posted no file). kind: registered (an RIA filing the full Form
-- ADV: regulatory AUM, accounts, employees) or exempt (an exempt reporting adviser - private-fund or VC adviser - with no
-- AUM in this file). aum_total = regulatory assets under management in US$ (sub-advised money is counted by every
-- adviser on it, so totals across firms overstate). aum_change_pct = vs the firm's previous snapshot; prev_kind shows a
-- move from exempt to registered. cik is set for ~25% of registered firms (those that also file with EDGAR).
WITH m AS (SELECT * FROM {{ source('sec', 'adv_firms') }} WHERE crd IS NOT NULL AND crd != ''
           QUALIFY ROW_NUMBER() OVER (PARTITION BY crd, as_of ORDER BY IF(kind = 'registered', 0, 1), ingested_at DESC) = 1)
SELECT as_of, kind, LAG(kind) OVER w AS prev_kind, crd, sec_number, cik, firm_name, legal_name, city, state, country,
       adv_filing_date, website, employees, aum_total, aum_discretionary, aum_nondiscretionary, accounts_total,
       LAG(aum_total) OVER w AS prev_aum, ROUND(100 * SAFE_DIVIDE(aum_total - LAG(aum_total) OVER w, LAG(aum_total) OVER w), 2) AS aum_change_pct
FROM m WINDOW w AS (PARTITION BY crd ORDER BY as_of)
