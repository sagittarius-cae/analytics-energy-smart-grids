-- ============================================================
-- 6. MDM
-- ============================================================

CREATE OR REPLACE VIEW dim_data_mgmt_system AS
SELECT system_id , 
       provider_id, 
       storage_type, 
       analytics_engine

FROM read_parquet('/workspace/data/3_cleaned/data_mgmt_system.parquet')