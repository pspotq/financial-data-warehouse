-- open-market Form 4 purchases with the noise removed: real tickers only, no 10b5-1 plan, private placement, ESPP,
-- DSU/fee-in-lieu grant, DRIP or foreign parallel-offering lines, no amendments, no implausible values, $10K minimum
SELECT * FROM {{ ref('insider_trades') }}
WHERE code = 'P' AND form = '4' AND NOT value_suspect
  AND REGEXP_CONTAINS(ticker, r'^[A-Z]{1,5}([.-][A-Z])?$') AND ticker NOT IN ('NONE', 'NA')
  AND NOT IFNULL(plan_10b5_1, FALSE) AND NOT IFNULL(private_placement, FALSE) AND NOT IFNULL(non_usd_suspect, FALSE)
  AND value >= 10000
  AND NOT REGEXP_CONTAINS(LOWER(IFNULL(footnotes, '')),
      r'employee stock purchase|espp|deferred stock unit|\bdsus?\b|dividend reinvest|\bdrip\b|grant date fair value|in lieu of cash|director fees|settlement date, which is the date the price per share|net asset value|nav per share|parallel offering|euro |€|brl|r\$')
