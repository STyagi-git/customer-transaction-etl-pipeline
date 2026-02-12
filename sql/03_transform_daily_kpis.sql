CREATE OR REPLACE TABLE `{{PROJECT}}.{{CURATED}}.daily_kpis` AS
SELECT
  DATE(transaction_ts) AS txn_date,
  COUNT(*) AS txn_count,
  COUNT(DISTINCT customer_id) AS active_customers,
  SUM(amount) AS revenue,
  AVG(amount) AS avg_order_value
FROM `{{PROJECT}}.{{RAW}}.transactions_raw`
GROUP BY txn_date
ORDER BY txn_date;
