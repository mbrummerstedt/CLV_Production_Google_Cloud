-- Start definition of temporary tables
WITH
-- Temporary table 1: Get all positive orders in their local currency 
order_line_local_currency AS (
SELECT  OrderId, DATE(CreatedOn) AS order_date, BillToEmail as userID, CAST(ROUND(SUM(AmountWithoutVat-DiscountAmountWithoutVat),2) AS FLOAT64) AS order_revenue, CurrencyCode
FROM `your-project.your-dataset.DocumentLine`
INNER JOIN `your-project.your-dataset.Document`
ON `your-project.your-dataset.DocumentLine`.DocumentId = `your-project.your-dataset.Document`.Id
WHERE
UNIX_SECONDS(`your-project.your-dataset.DocumentLine`.__ts_ms) = (SELECT MIN(UNIX_SECONDS(__ts_ms)) FROM `your-project.your-dataset.DocumentLine` dl2 WHERE `your-project.your-dataset.DocumentLine`.Id = dl2.Id)
AND UNIX_SECONDS(`your-project.your-dataset.Document`.__ts_ms) = (SELECT MIN(UNIX_SECONDS(__ts_ms)) FROM `your-project.your-dataset.Document` o2 WHERE `your-project.your-dataset.Document`.OrderID = o2.OrderID)
AND AmountWithoutVat > 0
AND ItemId NOT LIKE "P%"
GROUP BY OrderId, userID, order_date, CurrencyCode
),
-- Temporary table 2: Convert all positive orders to DKK
order_line_converted_to_dkk AS (
SELECT OrderId, order_date, userID , CAST(ROUND(order_revenue/Rate,2) AS FLOAT64) AS order_revenue
FROM order_line_local_currency
LEFT JOIN `your-project.bi_stage_dataset.Exchange_rates` Exchange_rates
ON order_line_local_currency.CurrencyCode = Exchange_rates.CurrencyCode AND order_line_local_currency.order_date = Exchange_rates.Date
),
-- Temporary table 3: Get all return orders in their local currency
return_line_local_currency AS (
SELECT `your-project.your-dataset.DocumentLine`.SalesOrderId, DATE(CreatedOn) AS return_date, CAST(ROUND(SUM(AmountWithoutVat-DiscountAmountWithoutVat),2) AS FLOAT64) AS return_amaount,
CurrencyCode
FROM `your-project.your-dataset.DocumentLine`
INNER JOIN `your-project.your-dataset.Document`
ON `your-project.your-dataset.DocumentLine`.DocumentId = `your-project.your-dataset.Document`.Id
WHERE
UNIX_SECONDS(`your-project.your-dataset.DocumentLine`.__ts_ms) = (SELECT MAX(UNIX_SECONDS(__ts_ms)) FROM `your-project.your-dataset.DocumentLine` dl2 WHERE `your-project.your-dataset.DocumentLine`.Id = dl2.Id)
AND UNIX_SECONDS(`your-project.your-dataset.Document`.__ts_ms) = (SELECT MAX(UNIX_SECONDS(__ts_ms)) FROM `your-project.your-dataset.Document` o2 WHERE `your-project.your-dataset.Document`.OrderID = o2.OrderID)
AND AmountWithoutVat < 0
AND ItemId NOT LIKE "P%"
GROUP BY SalesOrderId, return_date, CurrencyCode
),
-- Temporary table 4: Convert all return orders to DKK
return_line_converted_to_dkk AS (
SELECT SalesOrderId, return_date , CAST(ROUND(return_amaount/Rate,2) AS FLOAT64) AS return_amaount
FROM return_line_local_currency
LEFT JOIN `your-project.bi_stage_dataset.Exchange_rates` Exchange_rates
ON return_line_local_currency.CurrencyCode = Exchange_rates.CurrencyCode AND return_line_local_currency.return_date = Exchange_rates.Date
),
orders_with_returns_included AS (
-- Temporary table 5: Join returns and orders in DKK to get the final revenue from each order
SELECT OrderId, order_line_converted_to_dkk.userId, order_date, 
(CASE 
WHEN return_amaount IS NOT NULL THEN CAST(ROUND(order_revenue+return_amaount, 2) AS FLOAT64)
ELSE order_revenue END) order_value

FROM order_line_converted_to_dkk
LEFT JOIN return_line_converted_to_dkk
ON order_line_converted_to_dkk.OrderId = return_line_converted_to_dkk.SalesOrderId
),

number_of_orders_all_time AS (
-- Temporary table 6: Number of orders in the training period
SELECT userId, COUNT(order_date) AS number_of_orders 
FROM orders_with_returns_included
WHERE order_value >= 0
GROUP BY userID
),

number_of_orders_in_the_last_two_years AS (
-- Temporary table 7: Number of orders in the training period
SELECT userID, COUNT(order_date) AS number_of_orders 
FROM orders_with_returns_included
WHERE order_value >= 0
AND order_date > DATE_SUB(CURRENT_DATE("Europe/Copenhagen"), INTERVAL 24 MONTH)
GROUP BY userId
),
-- Temporary table 8: Customers who have bought since yesterday
customers_who_bought_within_the_last_day AS (
SELECT DISTINCT BillToEmail AS userId 
FROM `your-project.your-dataset.Document` 
WHERE DATE(__ts_ms) >= DATE_SUB(CURRENT_DATE("Europe/Copenhagen"), INTERVAL 1 DAY)
),

-- Tempoary table 9: The dataset used for the training_df
training_df AS (
SELECT orders_with_returns_included.userId, order_date, order_value
FROM orders_with_returns_included


INNER JOIN (
-- We only want to keep customers who bought before at least two times before the threshold date
SELECT number_of_orders_all_time.userId
FROM number_of_orders_all_time
INNER JOIN number_of_orders_in_the_last_two_years
ON number_of_orders_all_time.userId = number_of_orders_in_the_last_two_years.userId
WHERE number_of_orders_all_time.number_of_orders >= 2
AND number_of_orders_in_the_last_two_years.number_of_orders >= 1
) customers_with_at_two_least_purchases
ON orders_with_returns_included.userId = customers_with_at_two_least_purchases.userId

--We only want to make new daily predictions for customers who have made a new order
INNER JOIN customers_who_bought_within_the_last_day
ON orders_with_returns_included.userId = customers_who_bought_within_the_last_day.userId

WHERE order_value > 0 -- We do not want to include orders with a negative revenue or fully refunded. 
GROUP BY OrderId, orders_with_returns_included.userID, order_date, order_value
ORDER BY order_date DESC
)
-- End temporary tables definition / Start Main query
SELECT userId, SUM(order_value) AS current_total_revenue 
FROM training_df
GROUP BY userId
ORDER BY userId
