-- ============================================================
-- 7. Metering
-- ============================================================

CREATE OR REPLACE VIEW dim_metering AS
SELECT
    h.hes_id, 
    h.network_type, 
    h.coverage_area,
    d.system_id AS "dms_system_id", 
    d.storage_type AS "dms_storage_type",
    d.analytics_engine AS "dms_analytics_engine",
    p.provider_id, 
    p.name AS "provider_name", 
    p.region AS "provider_region"
FROM read_parquet('/workspace/data/3_cleaned/ami_head_end.parquet') h
LEFT JOIN dim_data_mgmt_system d ON d.system_id = h."dms_system_id"
LEFT JOIN dim_utility_provider p ON p."provider_id" = d.provider_id;