-- ============================================================
-- SILVER: Customer Dimension (Auto CDC - SCD Type 1)
-- ============================================================
-- Processes INSERT/UPDATE/DELETE operations from the raw CDC feed
-- to maintain the current state of the customer dimension.
--
-- Expectations:
--   valid_customer_id  : PK cannot be null        -> FAIL UPDATE
--   valid_email_format : basic email validation    -> WARN
--   valid_risk_rating  : enum check                -> DROP ROW
--   valid_kyc_status   : enum check                -> DROP ROW
--   valid_segment      : enum check                -> WARN
-- ============================================================

USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE customer (
  CONSTRAINT valid_customer_id   EXPECT (customer_id IS NOT NULL)                                                    ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_email_format  EXPECT (email LIKE '%@%.%'),
  CONSTRAINT valid_risk_rating   EXPECT (risk_rating IN ('LOW', 'MEDIUM', 'HIGH'))                                   ON VIOLATION DROP ROW,
  CONSTRAINT valid_kyc_status    EXPECT (kyc_status IN ('VERIFIED', 'PENDING', 'EXPIRED'))                           ON VIOLATION DROP ROW,
  CONSTRAINT valid_segment       EXPECT (customer_segment IN ('Mass', 'Mass Affluent', 'Preferred', 'Private'))
)
COMMENT 'Clean customer dimension - SCD Type 1 current state from CDC feed';

CREATE FLOW customer_cdc AS AUTO CDC INTO customer
FROM STREAM(bronze.raw_customer)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY event_timestamp
COLUMNS * EXCEPT (operation, event_timestamp, _source_file_path, _source_file_name, _ingested_at)
STORED AS SCD TYPE 1;
