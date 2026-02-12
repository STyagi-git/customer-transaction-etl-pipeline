CREATE OR REPLACE TABLE `{{PROJECT}}.{{CURATED}}.customer_summary` AS
WITH tx AS (
  SELECT
    customer_id,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_spend,
    AVG(amount) AS avg_txn_value,
    MIN(transaction_ts) AS first_txn_ts,
    MAX(transaction_ts) AS last_txn_ts
  FROM `{{PROJECT}}.{{RAW}}.transactions_raw`
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.signup_date,
  c.country,
  COALESCE(tx.txn_count, 0) AS txn_count,
  COALESCE(tx.total_spend, 0) AS total_spend,
  tx.avg_txn_value,
  tx.first_txn_ts,
  tx.last_txn_ts,
  CASE
    WHEN tx.last_txn_ts IS NULL THEN "NO_TXNS"
    WHEN tx.last_txn_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) THEN "ACTIVE_30D"
    ELSE "INACTIVE"
  END AS activity_status
FROM `{{PROJECT}}.{{RAW}}.customers_raw` c
LEFT JOIN tx
USING (customer_id);
