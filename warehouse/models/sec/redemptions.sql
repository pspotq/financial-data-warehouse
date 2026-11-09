-- calls and redemptions announced in 8-Ks (sec.redemption_terms, rule-based; b172): one row per filing that announces
-- a call of the issuer's own notes, preferred or warrants, newest extractor version. security / security_kind / full_call /
-- redemption_date / price_pct (% of principal or liquidation preference) / price_per_unit ($ per share or warrant) /
-- make_whole; call_text = the announcing sentence and what follows. Security and date are reliable; amount_usd can be a
-- series' size rather than the amount called - check the filing before quoting it. Ticker from the filer field.
SELECT * EXCEPT (extracted_at, is_call), REGEXP_EXTRACT(filer, r'\(([A-Z]{1,5}(?:[.-][A-Z]{1,2})?)[,)]') AS ticker,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', cik, '/', acc18, '/', accession, '-index.htm') AS index_url
FROM (SELECT * FROM {{ source('sec', 'redemption_terms') }} WHERE TRUE
      QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18 ORDER BY extractor_version DESC, extracted_at DESC) = 1)
WHERE is_call
