-- ============================================================
-- SILVER: Account Dimension (Cleaned & Validated)
-- ============================================================
-- Expectations:
--   valid_account_id      : PK cannot be null              -> FAIL UPDATE
--   valid_customer_ref    : FK to customer cannot be null   -> FAIL UPDATE
--   valid_status          : enum check                      -> DROP ROW
--   valid_account_type    : enum check                      -> DROP ROW
--   non_negative_balance  : balance sanity check            -> WARN
--   available_lte_current : business rule monitoring        -> WARN
-- ============================================================

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE account (
  CONSTRAINT valid_account_id      EXPECT (account_id IS NOT NULL)                                    ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_ref    EXPECT (customer_id IS NOT NULL)                                   ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_status          EXPECT (status IN ('Active', 'Closed', 'Blocked', 'Dormant'))      ON VIOLATION DROP ROW,
  CONSTRAINT valid_account_type    EXPECT (account_type IN ('Savings', 'Current', 'NRE', 'Salary'))   ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_balance  EXPECT (current_balance >= 0),
  CONSTRAINT available_lte_current EXPECT (available_balance <= current_balance)
)
COMMENT 'Clean account dimension with standardized types and quality checks'
AS SELECT
  account_id,
  customer_id,
  product_id,
  branch_id,
  account_type,
  currency,
  CAST(opening_date AS DATE)               AS opening_date,
  CAST(current_balance AS DECIMAL(18,2))   AS current_balance,
  CAST(available_balance AS DECIMAL(18,2)) AS available_balance,
  status,
  CAST(updated_at AS TIMESTAMP)            AS updated_at
FROM STREAM(bronze.raw_account);
