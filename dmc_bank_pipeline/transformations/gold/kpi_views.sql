-- ============================================================
-- GOLD LAYER: Business-Ready KPIs & Metrics
-- ============================================================
-- Materialized views for reporting and analytics.
-- Reads from Silver layer using batch semantics.
-- ============================================================

USE SCHEMA gold;

-- ----------------------------------------------------------
-- 1. Transaction Summary by Type, Status, Direction
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW transaction_summary
COMMENT 'Aggregated transaction KPIs by type, status, and direction'
AS SELECT
  transaction_type,
  transaction_status,
  direction,
  COUNT(*)    AS transaction_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount,
  MIN(amount) AS min_amount,
  MAX(amount) AS max_amount
FROM silver.transaction
GROUP BY ALL;

-- ----------------------------------------------------------
-- 2. Daily Transaction Trends (successful transactions only)
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW daily_transaction_trends
COMMENT 'Daily transaction trends for time-series analysis'
AS SELECT
  DATE(transaction_timestamp) AS transaction_date,
  transaction_type,
  channel,
  COUNT(*)                    AS transaction_count,
  SUM(amount)                 AS total_amount,
  AVG(amount)                 AS avg_amount,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT account_id)  AS unique_accounts
FROM silver.transaction
WHERE transaction_status = 'SUCCESS'
GROUP BY ALL;

-- ----------------------------------------------------------
-- 3. Customer 360 View (pre-aggregated CTEs to avoid fan-out)
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW customer_360
COMMENT 'Customer-level 360 view with account and transaction metrics'
AS
WITH account_agg AS (
  SELECT
    customer_id,
    COUNT(*)                                          AS total_accounts,
    COUNT(CASE WHEN status = 'Active' THEN 1 END)     AS active_accounts,
    SUM(current_balance)                               AS total_balance
  FROM silver.account
  GROUP BY customer_id
),
transaction_agg AS (
  SELECT
    customer_id,
    COUNT(*)                                                        AS total_transactions,
    SUM(CASE WHEN direction = 'DEBIT'  THEN amount ELSE 0 END)     AS total_debits,
    SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE 0 END)     AS total_credits,
    AVG(amount)                                                     AS avg_transaction_amount
  FROM silver.transaction
  WHERE transaction_status = 'SUCCESS'
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.city,
  c.state,
  c.customer_segment,
  c.risk_rating,
  c.kyc_status,
  COALESCE(a.total_accounts, 0)          AS total_accounts,
  COALESCE(a.active_accounts, 0)         AS active_accounts,
  COALESCE(a.total_balance, 0)           AS total_balance,
  COALESCE(t.total_transactions, 0)      AS total_transactions,
  COALESCE(t.total_debits, 0)            AS total_debits,
  COALESCE(t.total_credits, 0)           AS total_credits,
  COALESCE(t.avg_transaction_amount, 0)  AS avg_transaction_amount
FROM silver.customer c
LEFT JOIN account_agg a ON c.customer_id = a.customer_id
LEFT JOIN transaction_agg t ON c.customer_id = t.customer_id;

-- ----------------------------------------------------------
-- 4. Account-Level Transaction Metrics
-- ----------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW account_metrics
COMMENT 'Account-level transaction metrics for portfolio analysis'
AS SELECT
  a.account_id,
  a.customer_id,
  a.account_type,
  a.status                 AS account_status,
  a.current_balance,
  a.available_balance,
  COUNT(t.transaction_id)                                                      AS transaction_count,
  COALESCE(SUM(t.amount), 0)                                                   AS total_transaction_amount,
  COALESCE(AVG(t.amount), 0)                                                   AS avg_transaction_amount,
  COALESCE(SUM(CASE WHEN t.direction = 'DEBIT'  THEN t.amount ELSE 0 END), 0) AS total_debits,
  COALESCE(SUM(CASE WHEN t.direction = 'CREDIT' THEN t.amount ELSE 0 END), 0) AS total_credits,
  MAX(t.transaction_timestamp)                                                 AS last_transaction_date,
  MIN(t.transaction_timestamp)                                                 AS first_transaction_date
FROM silver.account a
LEFT JOIN silver.transaction t
  ON a.account_id = t.account_id
  AND t.transaction_status = 'SUCCESS'
GROUP BY ALL;
