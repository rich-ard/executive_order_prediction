DECLARE MAX_DATE TIMESTAMP;
SET MAX_DATE = (
    SELECT MAX(
      PARSE_TIMESTAMP('%Y-%m-%d', dt)
      )
    FROM `executive-orders-448515.presidential_approvals.presidential_approvals_ingest`
    WHERE _PARTITIONTIME > '2025-01-20'
    );

CREATE OR REPLACE TABLE `executive-orders-448515.presidential_approvals.presidential_approvals_sort` AS 
SELECT
  # basic format cleaning of week_start
  COALESCE(
    SAFE.PARSE_DATE('%m/%d/%y', start_date)   #1/10/21
    , SAFE.PARSE_DATE('%m/%d/%Y', start_date) #1/10/2021
    , SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_REPLACE(start_date, r'^(?:.*?\\K/){3}', '')) #1/10/2/021 for some reason
  )
  AS week_start
  , COALESCE(
    SAFE.PARSE_DATE('%m/%d/%y', end_date)   #1/10/21
    , SAFE.PARSE_DATE('%m/%d/%Y', end_date) #1/10/2021
    , SAFE.PARSE_DATE('%m/%d/%Y', REGEXP_REPLACE(end_date, r'^(?:.*?\\K/){3}', '')) #1/10/2/021 for some reason
  )
  AS week_end
  , approving
  , disapproving
  , unsure_or_no_data
  , president
FROM `executive-orders-448515.presidential_approvals.presidential_approvals_ingest`
WHERE 1=1
  AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = MAX_DATE
  AND (start_date IS NOT NULL or end_date IS NOT NULL)

