DECLARE MAX_DATE TIMESTAMP;
SET MAX_DATE = (
    SELECT MAX(
      PARSE_TIMESTAMP('%Y-%m-%d', dt)
      )
    FROM `executive-orders-448515.economic_indicators.economic_indicators_ingest`
    WHERE _PARTITIONTIME > '2025-01-20'
    );

CREATE OR REPLACE TABLE `executive-orders-448515.economic_indicators.economic_indicators_sort` AS 
SELECT
   PARSE_DATE('%m-%Y', CONCAT(Month,'-',Year)) AS month_start
  , * EXCEPT(Month, Year, dt, lang)
FROM `executive-orders-448515.economic_indicators.economic_indicators_ingest`
WHERE 1=1
  AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = MAX_DATE
ORDER BY month_start DESC

