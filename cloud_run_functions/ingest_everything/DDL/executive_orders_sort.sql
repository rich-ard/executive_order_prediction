DECLARE MAX_DATE TIMESTAMP;
SET MAX_DATE = (
    SELECT MAX(
      PARSE_TIMESTAMP('%Y-%m-%d', dt)
      )
    FROM `executive-orders-448515.executive_orders.executive_orders_ingest`
    WHERE _PARTITIONTIME > '2025-01-20'
    );

CREATE OR REPLACE TABLE `executive-orders-448515.executive_orders.executive_orders_sort` AS 
SELECT
  publication_date
  , COUNT(*) AS executive_orders_on_this_date
FROM `executive-orders-448515.executive_orders.executive_orders_ingest`
WHERE 1=1
  AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = MAX_DATE
GROUP BY publication_date
ORDER BY publication_date DESC

