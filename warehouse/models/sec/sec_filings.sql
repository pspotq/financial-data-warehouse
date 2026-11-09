-- one row per filing per EDGAR full-text-search stream, newest first. Primary documents only (file_type equals
-- the form) unless the stream only matches exhibits; doc_url opens the matched document, index_url the filing index.
SELECT query_id AS stream, file_date, form, accession, display_names AS filer, ciks, items, file_type,
       file_description, biz_locations, index_url, doc_url, first_seen
FROM {{ source('sec', 'filing_hits') }} WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY query_id, accession, file_date ORDER BY (file_type = form) DESC, first_seen) = 1
