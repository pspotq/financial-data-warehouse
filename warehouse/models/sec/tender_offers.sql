-- one row per tender offer: the original Schedule TO with what its amendments added (same filer directory CIK and
-- form family, filed within 180 days after, before the next original): latest expiration, final counts, final price
WITH t AS (SELECT * FROM {{ ref('tender_terms') }}),
o AS (SELECT *, LEAD(file_date) OVER (PARTITION BY cik, form ORDER BY file_date, acc18) AS next_original
      FROM t WHERE NOT ENDS_WITH(form, '/A')),
a AS (SELECT o.acc18 AS orig, t.* FROM o JOIN t ON t.cik = o.cik AND t.form = CONCAT(o.form, '/A')
        AND t.file_date >= o.file_date AND t.file_date <= DATE_ADD(o.file_date, INTERVAL 180 DAY)
        AND (o.next_original IS NULL OR t.file_date < o.next_original)),
agg AS (SELECT orig, COUNT(*) AS amendments, MAX(file_date) AS last_amended,
          ARRAY_AGG(new_expiration_date IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS extended_to,
          ARRAY_AGG(shares_tendered IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS shares_tendered,
          ARRAY_AGG(shares_accepted IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS shares_accepted,
          ARRAY_AGG(final_price IGNORE NULLS ORDER BY file_date DESC LIMIT 1)[SAFE_OFFSET(0)] AS final_price,
          LOGICAL_OR(is_final) AS final_reported
        FROM a GROUP BY orig)
SELECT o.acc18, o.accession, o.form, o.file_date, o.cik, o.subject_company, o.filer, o.security_class, o.cusip, o.offer_kind,
       o.price_per_share, o.price_low, o.price_high, o.at_nav, o.dutch_auction, o.max_shares, o.max_pct, o.max_amount_usd,
       o.transaction_value_usd, o.expiration_date, agg.extended_to, COALESCE(agg.extended_to, o.expiration_date) AS last_expiration,
       IFNULL(agg.amendments, 0) AS amendments, agg.last_amended, agg.shares_tendered, agg.shares_accepted, agg.final_price,
       IFNULL(agg.final_reported, FALSE) AS final_reported,
       CONCAT('https://www.sec.gov/Archives/edgar/data/', o.cik, '/', o.acc18, '/', o.accession, '-index.htm') AS index_url
FROM o LEFT JOIN agg ON agg.orig = o.acc18
