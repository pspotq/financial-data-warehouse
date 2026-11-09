-- shares outstanding per company per report, point in time: the cover-page figure (dei) when reported, else the
-- balance-sheet figure. known_from = the filing date (frames rows, which carry none, use period end + 45 days).
-- Companies with several share classes may show one class only.
SELECT cik, entity, period_end, val AS shares, concept, accn, form, filed,
       COALESCE(filed, DATE_ADD(period_end, INTERVAL 45 DAY)) AS known_from
FROM {{ source('sec', 'xbrl_facts') }}
WHERE unit = 'shares' AND concept IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding') AND val > 0
  AND period_end BETWEEN DATE '1993-01-01' AND DATE_ADD(COALESCE(filed, CURRENT_DATE()), INTERVAL 400 DAY)
QUALIFY ROW_NUMBER() OVER (PARTITION BY cik, period_end, accn
                           ORDER BY concept = 'EntityCommonStockSharesOutstanding' DESC, source = 'companyfacts' DESC) = 1
