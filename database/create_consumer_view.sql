-- ============================================================
-- 2. Customer Premises
-- ============================================================

CREATE OR REPLACE VIEW dim_consumer AS
SELECT consumer_id, 
       account_type, 
       address
FROM read_parquet('./data/3_cleaned/consumer.parquet');