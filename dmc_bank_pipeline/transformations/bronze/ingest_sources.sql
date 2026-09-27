-- ============================================================
-- BRONZE LAYER: Metadata-Driven Incremental Ingestion
-- ============================================================
-- Source Registry:
-- +------------------+----------------------+--------------+
-- | Table Name       | Source Subdirectory   | Format       |
-- +------------------+----------------------+--------------+
-- | raw_customer     | /customer            | CSV (header) |
-- | raw_account      | /account             | CSV (header) |
-- | raw_transaction  | /transaction         | CSV (header) |
-- +------------------+----------------------+--------------+
--
-- Ingestion Pattern (identical for every source):
--   1. STREAM + read_files (Auto Loader) for incremental CSV
--   2. Schema inference enabled (inferColumnTypes => true)
--   3. All source columns preserved as-is (schema-on-read)
--   4. Ingestion metadata appended:
--      - _source_file_path  (full file path)
--      - _source_file_name  (file name only)
--      - _ingested_at       (UTC ingestion timestamp)
--
-- To onboard a new source: copy any block, change name/path/comment.
--
-- PARAMETERIZATION NOTE:
-- For environment promotion (DEV->STAGE->PROD), add a pipeline
-- parameter 'source_base_path' and replace the hardcoded Volume
-- path with:  :source_base_path || '/customer'  (etc.)
-- ============================================================

USE SCHEMA bronze;

-- ----------------------------------------------------------
-- Source: Customer CDC Feed
-- ----------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE raw_customer
COMMENT 'Raw customer CDC feed ingested from CSV via Auto Loader'
AS SELECT
  *,
  _metadata.file_path  AS _source_file_path,
  _metadata.file_name  AS _source_file_name,
  current_timestamp()  AS _ingested_at
FROM STREAM(read_files(
  '/Volumes/yt_dev/bronze/dmc_bank_raw/customer',
  format => 'csv',
  header => true,
  inferColumnTypes => true
));

-- ----------------------------------------------------------
-- Source: Account Seed
-- ----------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE raw_account
COMMENT 'Raw account seed data ingested from CSV via Auto Loader'
AS SELECT
  *,
  _metadata.file_path  AS _source_file_path,
  _metadata.file_name  AS _source_file_name,
  current_timestamp()  AS _ingested_at
FROM STREAM(read_files(
  '/Volumes/yt_dev/bronze/dmc_bank_raw/account',
  format => 'csv',
  header => true,
  inferColumnTypes => true
));

-- ----------------------------------------------------------
-- Source: Transaction Seed
-- ----------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE raw_transaction
COMMENT 'Raw transaction seed data ingested from CSV via Auto Loader'
AS SELECT
  *,
  _metadata.file_path  AS _source_file_path,
  _metadata.file_name  AS _source_file_name,
  current_timestamp()  AS _ingested_at
FROM STREAM(read_files(
  '/Volumes/yt_dev/bronze/dmc_bank_raw/transaction',
  format => 'csv',
  header => true,
  inferColumnTypes => true
));
