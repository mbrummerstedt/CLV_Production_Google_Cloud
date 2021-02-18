  -- This query is used to calculate the CLV and Churn probability Segment when new predictions are made. It is used both for weekly calculations where calculations are calculated all all customers we can make predictions for.
  -- Steps in query:
  -- Step 1: Calculate quantiles on the CLV of all customers with predictions
  -- Step 2: Canculate 25%, 50% and 75% percentiles
  -- Step 3: Set new customer Segments
  --Save to table with clv and churn predictions
CREATE OR REPLACE TABLE `your-project.customer_predictions.clv_and_churn_predictions`
CLUSTER BY userId AS
WITH
  -- Step 1: Calculate quantiles on the CLV of all customers with predictions
  all_customers_with_clv_predictions AS (
  SELECT
    userId,
    clv,
    churn_probability,
    predicted_value_next_6_month,
    current_total_revenue
  FROM
    `your-project.ml_models_production.new_predictions`),
  approx_quantiles_data AS (
  SELECT
    APPROX_QUANTILES(clv, 100) percentiles
  FROM
    all_customers_with_clv_predictions ),
  -- Step 2: Canculate 25%, 50% and 75% percentiles
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
  -- Step 3: Set new customer Segments
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
