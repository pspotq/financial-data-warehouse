-- one row per security (CUSIP) per quarter across every 13F manager: holders, shares and value in common / long
-- positions (put_call NULL, shares_type SH), plus call and put option value, with the change vs the prior quarter.
-- Several managers can report the same shares (a parent and its sub-adviser, other_manager), so totals can overstate;
-- compare a security with its own history. Heavy: scans the holdings for the quarters asked; filter on period or cusip.
WITH s AS (
  SELECT period, cusip, ANY_VALUE(issuer) AS issuer, ANY_VALUE(title_of_class) AS title_of_class,
         COUNT(DISTINCT IF(put_call IS NULL, cik, NULL)) AS holders,
         SUM(IF(put_call IS NULL AND shares_type = 'SH', shares, 0)) AS shares,
         SUM(IF(put_call IS NULL, value_usd, 0)) AS value_usd,
         SUM(IF(UPPER(put_call) = 'CALL', value_usd, 0)) AS call_value_usd,
         SUM(IF(UPPER(put_call) = 'PUT', value_usd, 0)) AS put_value_usd
  FROM {{ ref('form13f_holdings') }} GROUP BY period, cusip)
SELECT *, holders - LAG(holders) OVER w AS holders_change, shares - LAG(shares) OVER w AS shares_change,
       ROUND(100 * SAFE_DIVIDE(shares - LAG(shares) OVER w, LAG(shares) OVER w), 2) AS shares_change_pct
FROM s WINDOW w AS (PARTITION BY cusip ORDER BY period)
