-- federal debt against the statutory limit per business day (DTS Table IIIC), $ millions, Oct 2005 on.
-- subject_to_limit = held by the public + intragovernmental - debt not subject to the limit + other debt subject to it.
-- statutory_limit is NULL while the limit is suspended (limit_suspended = TRUE); headroom = limit - subject_to_limit.
WITH d AS (
  SELECT record_date,
    SUM(IF(category = 'Debt Held by the Public', close_today_bal, 0)) AS held_by_public,
    SUM(IF(category = 'Intragovernmental Holdings', close_today_bal, 0)) AS intragovernmental,
    SUM(IF(REGEXP_CONTAINS(category, r'Not Subject to Limit'), close_today_bal, 0)) AS not_subject,
    SUM(IF(REGEXP_CONTAINS(category, r'Other Debt Subject to Limit'), close_today_bal, 0)) AS other_subject,
    MAX(IF(category = 'Statutory Debt Limit', close_today_bal, NULL)) AS statutory_limit
  FROM {{ source('gov', 'treasury_dts') }} WHERE table_id = 'debt_limit' GROUP BY record_date)
SELECT record_date, held_by_public, intragovernmental, held_by_public + intragovernmental AS total_public_debt,
       held_by_public + intragovernmental - not_subject + other_subject AS subject_to_limit, statutory_limit,
       statutory_limit IS NULL AS limit_suspended,
       statutory_limit - (held_by_public + intragovernmental - not_subject + other_subject) AS headroom
FROM d
