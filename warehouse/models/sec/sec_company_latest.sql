-- one row per company: the newest reported value of each balance-sheet item (US$ unless noted) and its period end
WITH f AS (
  SELECT cik, concept, ARRAY_AGG(STRUCT(val, period_end, entity) ORDER BY period_end DESC, filed DESC LIMIT 1)[OFFSET(0)] AS x
  FROM {{ source('sec', 'xbrl_facts') }} WHERE period_start IS NULL AND unit IN ('USD', 'shares')
    AND period_end BETWEEN DATE '1993-01-01' AND DATE_ADD(COALESCE(filed, CURRENT_DATE()), INTERVAL 400 DAY) GROUP BY cik, concept)
SELECT cik, ANY_VALUE(x.entity) AS entity,
  COALESCE(MAX(IF(concept = 'EntityCommonStockSharesOutstanding', x.val, NULL)), MAX(IF(concept = 'CommonStockSharesOutstanding', x.val, NULL))) AS shares_outstanding,
  MAX(IF(concept = 'EntityPublicFloat', x.val, NULL)) AS public_float,
  COALESCE(MAX(IF(concept = 'CashAndCashEquivalentsAtCarryingValue', x.val, NULL)),
           MAX(IF(concept = 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents', x.val, NULL))) AS cash,
  COALESCE(MAX(IF(concept = 'ShortTermInvestments', x.val, NULL)), MAX(IF(concept = 'MarketableSecuritiesCurrent', x.val, NULL))) AS short_term_investments,
  MAX(IF(concept = 'AssetsCurrent', x.val, NULL)) AS current_assets, MAX(IF(concept = 'Assets', x.val, NULL)) AS total_assets,
  MAX(IF(concept = 'LiabilitiesCurrent', x.val, NULL)) AS current_liabilities, MAX(IF(concept = 'Liabilities', x.val, NULL)) AS total_liabilities,
  COALESCE(MAX(IF(concept = 'LongTermDebt', x.val, NULL)), MAX(IF(concept = 'LongTermDebtNoncurrent', x.val, NULL))) AS long_term_debt,
  COALESCE(MAX(IF(concept = 'DebtCurrent', x.val, NULL)), MAX(IF(concept = 'LongTermDebtCurrent', x.val, NULL))) AS debt_current,
  MAX(IF(concept = 'StockholdersEquity', x.val, NULL)) AS equity,
  MAX(IF(concept IN ('Assets', 'StockholdersEquity', 'Liabilities'), x.period_end, NULL)) AS balance_sheet_as_of
FROM f GROUP BY cik
