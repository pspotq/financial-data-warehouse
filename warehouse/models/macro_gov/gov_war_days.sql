-- days with a war.gov daily contracts announcement page
SELECT contract_date, title, link, published FROM {{ source('gov', 'war_contract_days') }}
