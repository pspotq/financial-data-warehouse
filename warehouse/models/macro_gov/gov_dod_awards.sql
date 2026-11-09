-- Defense contract transactions on awards of $100M+ from USAspending (posts with roughly a 90-day delay). The $100M
-- filter is on the award, not the transaction: most rows are smaller modifications and ~11% are negative de-obligations,
-- so filter amount_usd for "large contract" facts (Weekly Flows keeps >= $250M).
SELECT action_date, recipient, amount_usd, is_new_award, mod, award_id, sub_agency, description, url
FROM {{ source('gov', 'dod_awards') }}
