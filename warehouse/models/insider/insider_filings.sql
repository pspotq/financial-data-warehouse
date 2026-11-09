-- the Form 4 screen (see rrp.insider_filings_live for every column): the nightly copy rrp-feeds saves after each Form 4
-- day loads, partitioned by filed month and clustered by ticker and code, so a read filtered on filed costs megabytes.
-- snapped_at = when the copy was made. Same rows and columns as the live view as of that time.
SELECT * FROM {{ source('sec', 'insider_filings_snap') }}
