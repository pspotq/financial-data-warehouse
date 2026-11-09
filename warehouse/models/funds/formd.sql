-- every Form D / D/A (exempt offering notice: private raises, new hedge / PE / VC / crypto funds), 2008 on, one row per
-- filing. new_offering = an original D (not an amendment); amendments update amounts sold and investor counts.
-- fund_type is set for pooled investment funds (Hedge Fund, Private Equity Fund, Venture Capital Fund, Other Investment
-- Fund ...). Amounts in US$; total_offering is NULL when offering_indefinite. exemptions: 06b / 06c = Rule 506(b)/(c),
-- 04 = Rule 504, 3C / 3C.1 / 3C.7 = Investment Company Act exclusions. url opens the filing.
SELECT accession, filed, form, form = 'D' AND IFNULL(is_amendment, FALSE) = FALSE AS new_offering, cik, entity_name,
       entity_type, city, state, jurisdiction, year_of_inc, industry_group, fund_type, is_40act, revenue_range, nav_range,
       exemptions, is_amendment, prev_accession, first_sale_date, first_sale_yet_to_occur, more_than_one_year,
       security_types, min_investment, total_offering, offering_indefinite, total_sold, total_remaining, investors_count,
       has_nonaccredited, nonaccredited_count, sales_commissions, finders_fees, related_persons, source,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', cik, '/', accession, '/') AS url
FROM {{ source('sec', 'formd') }} WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY accession ORDER BY ingested_at DESC) = 1
