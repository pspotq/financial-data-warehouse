-- every 13F holding line per manager per quarter (period = quarter end the report covers), amendments resolved through
-- rrp.form13f_reports, 2013 Q3 on (data sets) plus filings since Aug 24 2026 parsed from EDGAR daily. value_usd in
-- dollars (pre-2023 filings reported thousands; converted). shares_type SH = shares, PRN = principal amount (bonds);
-- put_call set = an option position (value = the underlying's value). 13F covers US-listed long positions only (no
-- shorts, no cash). Filter on cusip and / or period: the table is clustered on cusip and partitioned by period.
SELECT h.period, h.cik, r.manager, h.accession, h.filed, r.role, h.line, h.issuer, h.title_of_class, h.cusip, h.figi,
       h.value_usd, h.shares, h.shares_type, h.put_call, h.discretion, h.other_manager, h.vote_sole, h.vote_shared, h.vote_none
FROM {{ source('sec', 'form13f_holdings') }} h JOIN {{ ref('form13f_reports') }} r ON r.accession = h.accession
