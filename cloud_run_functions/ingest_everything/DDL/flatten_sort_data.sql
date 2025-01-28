# Set variable for earliest executive order available, and get the Sunday of that week as a start point
DECLARE FirstOrderDate DATE
  DEFAULT (SELECT DATE_TRUNC(MIN(publication_date), WEEK(SUNDAY)) FROM `executive-orders-448515.executive_orders.executive_orders_sort`);

# Create date-partitioned table if not present
CREATE TABLE IF NOT EXISTS `executive-orders-448515.weekly_data_collected.weekly_variables_flattened` (
  week_start DATE
  , approving STRING
  , disapproving STRING
  , unsure_or_no_data STRING
  , BusinessApplications INT64
  , ConstructionSpending STRING
  , DurableGoodsNewOrders STRING
  , InternationalTrade_Exports STRING
  , ManuInventories STRING
  , ManuNewOrders STRING
  , NewHomesForSale STRING
  , NewHomesSold STRING
  , ResConstPermits INT64
  , ResConstUnitsCompleted INT64
  , ResConstUnitsStarted INT64
  , RetailInventories STRING
  , SalesForRetailAndFood INT64
  , WholesaleInventories STRING
  , orders_outcome_var INT64
  , run_date DATETIME
)
PARTITION BY run_date;

INSERT INTO `executive-orders-448515.weekly_data_collected.weekly_variables_flattened` (

# Generate week array
WITH Week_Array AS (
  SELECT week_start
  FROM 
    UNNEST(
      GENERATE_DATE_ARRAY(FirstOrderDate, CURRENT_DATE(), INTERVAL 7 DAY)
    ) AS week_start
  ORDER BY week_start DESC
  )

  SELECT
    week.week_start
    , approvals.approving
    , approvals.disapproving
    , approvals.unsure_or_no_data
    , indicators.BusinessApplications
    , IF(indicators.ConstructionSpending = 'NA', NULL, ConstructionSpending) ConstructionSpending
    , IF(indicators.DurableGoodsNewOrders = 'NA', NULL, DurableGoodsNewOrders) DurableGoodsNewOrders
    , IF(indicators.InternationalTrade_Exports = 'NA', NULL, InternationalTrade_Exports) InternationalTrade_Exports
    , IF(indicators.ManuInventories = 'NA', NULL, ManuInventories) ManuInventories
    , IF(indicators.ManuNewOrders = 'NA', NULL, ManuNewOrders) ManuNewOrders
    , IF(indicators.NewHomesForSale = 'NA', NULL, NewHomesForSale) NewHomesForSale
    , IF(indicators.NewHomesSold = 'NA', NULL, NewHomesSold) NewHomesSold
    , indicators.ResConstPermits
    , indicators.ResConstUnitsCompleted
    , indicators.ResConstUnitsStarted
    , IF(indicators.RetailInventories = 'NA', NULL, RetailInventories) RetailInventories
    , indicators.SalesForRetailAndFood
    , IF(indicators.WholesaleInventories = 'NA', NULL, WholesaleInventories) WholesaleInventories
    , SUM(IFNULL(orders.executive_orders_on_this_date, 0)) AS orders_outcome_var
    , CURRENT_DATETIME() AS run_date
  FROM Week_Array week
  LEFT JOIN `executive-orders-448515.executive_orders.executive_orders_sort` orders
    ON orders.publication_date BETWEEN
      week.week_start AND DATE_ADD(week.week_start, INTERVAL 6 DAY)
  LEFT JOIN `executive-orders-448515.presidential_approvals.presidential_approvals_sort` approvals
    ON DATE_TRUNC(approvals.period_start, MONTH) = DATE_TRUNC(week.week_start, MONTH)
  LEFT JOIN `executive-orders-448515.economic_indicators.economic_indicators_sort` indicators
    ON indicators.month_start = DATE_TRUNC(week.week_start, MONTH)

  GROUP BY
    week.week_start
    , approvals.approving
    , approvals.disapproving
    , approvals.unsure_or_no_data
    , indicators.BusinessApplications
    , indicators.ConstructionSpending
    , indicators.DurableGoodsNewOrders
    , indicators.InternationalTrade_Exports
    , indicators.ManuInventories
    , indicators.ManuNewOrders
    , indicators.NewHomesForSale
    , indicators.NewHomesSold
    , indicators.ResConstPermits
    , indicators.ResConstUnitsCompleted
    , indicators.ResConstUnitsStarted
    , indicators.RetailInventories
    , indicators.SalesForRetailAndFood
    , indicators.WholesaleInventories
  ORDER BY week.week_start DESC
);
