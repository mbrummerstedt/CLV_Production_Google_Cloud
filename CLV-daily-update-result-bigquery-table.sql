  -- This query is used to calculate the CLV and Churn probability Segment when new predictions are made. It is used for daily updates where only predictions are not calculated for all customers
  -- Steps in query:
  -- Step 1: Get excisting customers that has not been made new predictions for.
  -- Step 2: Union all new predictions with the excisting predictions.
  -- Step 3: Calculate quantiles on the CLV of all customers with predictions
  -- Step 4: Canculate 25%, 50% and 75% percentiles
  -- Step 5: Set new customer Segments

  --Save to table with clv and churn predictions
CREATE OR REPLACE TABLE `your-project.customer_predictions.clv_and_churn_predictions`
CLUSTER BY userId AS


WITH
  -- Step 1: Get excisting customers that has not been made new predictions for.
  excisting_customer_predictions_that_has_not_been_updated AS (
  SELECT
    excisting_predictions.userId,
    excisting_predictions.clv,
    excisting_predictions.churn_probability,
    excisting_predictions.predicted_value_next_6_month,
    excisting_predictions.current_total_revenue
  FROM
    `your-project.customer_predictions.clv_and_churn_predictions` AS excisting_predictions
  LEFT JOIN
    `your-project.ml_models_production.new_predictions` AS new_predictions
  ON
    excisting_predictions.userId = new_predictions.userId
  WHERE
    new_predictions.userId IS NULL ),
  -- Step 2: Union all new predictions with the excisting predictions.
  all_customers_with_clv_predictions AS (
  SELECT
    userId,
    clv,
    churn_probability,
    predicted_value_next_6_month,
    current_total_revenue
  FROM
    `your-project.ml_models_production.new_predictions`
  UNION DISTINCT
  SELECT
    userId,
    clv,
    churn_probability,
    predicted_value_next_6_month,
    current_total_revenue
  FROM
    excisting_customer_predictions_that_has_not_been_updated ),
  -- Step 3: Calculate quantiles on the CLV of all customers with predictions
  approx_quantiles_data AS (
  SELECT
    APPROX_QUANTILES(clv, 100) percentiles
  FROM
    all_customers_with_clv_predictions ),
  -- Step 4: Canculate 25%, 50% and 75% percentiles
  clv_percentiles AS (
  SELECT
    percentiles[
  OFFSET
    (25)] AS p25,
    percentiles[
  OFFSET
    (50)] AS p50,
    percentiles[
  OFFSET
    (75)] AS p75
  FROM
    approx_quantiles_data )
  -- Step 5: Set new customer Segments
SELECT
  userId,
  clv,
  churn_probability,
  predicted_value_next_6_month,
  current_total_revenue,
  (CASE
      WHEN clv < p25 THEN "Lowest 25% Customers"
      WHEN clv BETWEEN p25
    AND p50 THEN "Low Medium Value"
      WHEN clv BETWEEN p50 AND p75 THEN "High Medium Value"
      WHEN clv > p75 THEN "Top 25% Customers"
    ELSE
    ""
  END
    ) AS clv_segment,
  (CASE
      WHEN churn_probability < 0.25 THEN "Low Risk"
      WHEN churn_probability BETWEEN 0.25
    AND 0.70 THEN "Medium Risk"
      WHEN churn_probability > 0.7 THEN "High Risk"
    ELSE
    ""
  END
    ) AS churn_probability_segment
FROM
  all_customers_with_clv_predictions
CROSS JOIN
  clv_percentiles clv_percentiles