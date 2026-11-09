-- Schedule TO terms as columns (sec.tender_terms, rule-based extraction from the filing text; b170). One row per
-- filing, newest extractor version. offer_kind: acquisition (merger sub), mini_tender (MacKenzie, Comrit, Everest ...),
-- third_party_other, fund_repurchase (interval funds, BDCs, non-traded REITs: usually at NAV), dutch_auction,
-- fixed_price, exchange_offer, warrant_tender, option_exchange. Amendments (form ending /A) carry shares_tendered,
-- shares_accepted, final_price, new_expiration_date, is_final. Hand check (65 filings): price, expiration and is_final
-- right 93-100%; size (max_*) right about half the time. debt_or_preferred / exchange offers carry no per-share price.
SELECT * EXCEPT (extracted_at) FROM {{ source('sec', 'tender_terms') }} WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18 ORDER BY extractor_version DESC, extracted_at DESC) = 1
