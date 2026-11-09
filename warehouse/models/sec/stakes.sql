-- Schedule 13D stakes as columns (sec.stake_terms, rule-based extraction from the filing text; b171). One row per
-- filing (SC 13D and, from Dec 2024, SCHEDULE 13D, with amendments), newest extractor version. pct / shares / top_filer
-- = the largest stake on the cover page; persons = every reporting person (name, funds, shares, pct, type);
-- purpose_flags = keywords in Item 4: board_seat, proxy_fight, sale_or_merger, letter_to_board, cooperation_agreement,
-- investment_only. funds codes: WC working capital, PF personal funds, AF affiliate, OO other, BK bank.
SELECT * EXCEPT (extracted_at) FROM {{ source('sec', 'stake_terms') }} WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18 ORDER BY extractor_version DESC, extracted_at DESC) = 1
