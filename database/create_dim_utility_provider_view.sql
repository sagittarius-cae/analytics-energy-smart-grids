-- ============================================================
-- 1. Corporate Master Data
-- ============================================================
CREATE OR REPLACE VIEW dim_utility_provider AS
SELECT provider_id, 
       name, 
       region
FROM read_parquet('/workspace/data/3_cleaned/utility_provider.parquet');