-- ============================================================
-- 3. Bulk Generation — CAMBIO: power_plant ya no va con renewable_source
-- ============================================================

CREATE OR REPLACE VIEW dim_bulk_generation AS
SELECT
    pp.plant_id, pp.type, 
    pp.capacity_mw,
    p.provider_id, 
    p.name AS "provider_name", 
    p.region AS "provider_region"
FROM read_parquet('./data/3_cleaned/power_plants.parquet') pp
LEFT JOIN read_parquet('./data/3_cleaned/utility_provider.parquet') p ON p.provider_id = pp.provider_id;