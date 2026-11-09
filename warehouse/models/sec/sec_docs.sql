-- filing text: one row per document, newest fetch per document. doc_type INDEX = the filing's document list
-- (seq | type | description | name | size); FORM4_XML = a Form 4's ownership XML; otherwise the EDGAR document type
-- (8-K, SC TO-I, SCHEDULE 13D, EX-99.1, EX-2.1 ...). Filter on acc18 (18-digit accession) AND a file_date range: since
-- 2026-10-29 sec.filing_docs requires a file_date filter (a read without one fails; one by acc18 alone billed ~16 GB).
SELECT accession, acc18, form, file_date, cik, seq, doc_type, description, doc_name, url, size_bytes, chars, truncated, text, source, fetched_at
FROM {{ source('sec', 'filing_docs') }} WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY acc18, file_date, doc_name ORDER BY fetched_at DESC) = 1
