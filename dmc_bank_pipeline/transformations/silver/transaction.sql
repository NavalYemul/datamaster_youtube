-- ============================================================
-- SILVER: Transaction Fact Table (Cleaned & Validated)
-- ============================================================
-- Expectations:
--   valid_transaction_id     : PK cannot be null           -> FAIL UPDATE
--   valid_account_ref        : FK to account cannot be null -> DROP ROW
--   valid_amount             : must be positive             -> DROP ROW
--   valid_direction          : enum check                   -> DROP ROW
--   valid_transaction_status : enum check                   -> DROP ROW
--   valid_timestamp          : timestamp cannot be null     -> DROP ROW
-- ============================================================

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE transaction (
  CONSTRAINT valid_transaction_id     EXPECT (transaction_id IS NOT NULL)                                     ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_account_ref        EXPECT (account_id IS NOT NULL)                                         ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount             EXPECT (amount > 0)                                                     ON VIOLATION DROP ROW,
  CONSTRAINT valid_direction          EXPECT (direction IN ('DEBIT', 'CREDIT'))                               ON VIOLATION DROP ROW,
  CONSTRAINT valid_transaction_status EXPECT (transaction_status IN ('SUCCESS', 'REVERSED', 'FAILED'))        ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp          EXPECT (transaction_timestamp IS NOT NULL)                               ON VIOLATION DROP ROW
)
COMMENT 'Clean transaction fact table with standardized types and quality checks'
AS SELECT
  transaction_id,
  account_id,
  customer_id,
  CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
  transaction_type,
  channel,
  direction,
  CAST(amount AS DECIMAL(18,2))            AS amount,
  currency,
  COALESCE(merchant_name, 'N/A')           AS merchant_name,
  transaction_status,
  reference_number
FROM STREAM(bronze.raw_transaction);
