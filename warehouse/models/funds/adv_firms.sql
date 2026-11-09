-- every SEC investment adviser in the newest monthly snapshot, one row per firm, with its history: first_listed = the first
-- snapshot it appears in (2019-02-01 = it was already there when the data starts), first_registered = first month as a
-- registered adviser (an exempt adviser that later registered shows both), months_listed, aum_1y_ago and
-- aum_1y_change_pct (vs the snapshot nearest 12 months earlier). last_13f_period / last_13f_value_usd = the firm's
-- newest 13F report (rrp.form13f_managers), joined on SEC CIK, so only for firms whose ADV gives a CIK. Firms that left
-- (deregistered, merged) are not here: see rrp.adv_firm_months. Reads the whole history (~420 MB).
WITH m AS (SELECT * FROM {{ ref('adv_firm_months') }}),
lat AS (SELECT MAX(as_of) AS d, MIN(as_of) AS d0 FROM m),
hist AS (SELECT crd, MIN(as_of) AS first_listed, MIN(IF(kind = 'registered', as_of, NULL)) AS first_registered,
                COUNT(*) AS months_listed FROM m GROUP BY crd),
yago AS (SELECT crd, aum_total AS aum_1y_ago FROM m, lat
         WHERE as_of BETWEEN DATE_SUB(lat.d, INTERVAL 14 MONTH) AND DATE_SUB(lat.d, INTERVAL 10 MONTH)
         QUALIFY ROW_NUMBER() OVER (PARTITION BY crd ORDER BY ABS(DATE_DIFF(as_of, DATE_SUB(lat.d, INTERVAL 12 MONTH), DAY))) = 1),
f13 AS (SELECT cik, ARRAY_AGG(STRUCT(period, value_usd) ORDER BY period DESC LIMIT 1)[OFFSET(0)] AS last
        FROM {{ ref('form13f_managers') }} GROUP BY cik)
SELECT m.as_of, m.kind, m.crd, m.sec_number, m.cik, m.firm_name, m.legal_name, m.city, m.state, m.country, m.adv_filing_date,
       m.website, m.employees, m.aum_total, m.aum_discretionary, m.accounts_total, y.aum_1y_ago,
       ROUND(100 * SAFE_DIVIDE(m.aum_total - y.aum_1y_ago, y.aum_1y_ago), 2) AS aum_1y_change_pct,
       h.first_listed, h.first_registered, h.months_listed, h.first_listed = lat.d0 AS listed_at_start,
       f.last.period AS last_13f_period, f.last.value_usd AS last_13f_value_usd
FROM m CROSS JOIN lat JOIN hist h USING (crd) LEFT JOIN yago y USING (crd) LEFT JOIN f13 f ON f.cik = m.cik
WHERE m.as_of = lat.d
