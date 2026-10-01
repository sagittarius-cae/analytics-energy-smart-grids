-- ============================================================
-- FACT 2: asset_capacity — snapshot de capacidad instalada,
-- unificando 5 medidas heterogéneas de forma segura (patrón EAV)
-- ============================================================

CREATE OR REPLACE VIEW fact_assets_capacity AS
SELECT "plant_id" AS "asset_id", 
       'bulk_generation' AS "domain", 
       'power_plant' AS "assest_type", 
       "capacity_mw" AS "value", 
       'mw' AS "unit" 
FROM read_parquet('/workspace/data/3_cleaned/power_plants.parquet')

UNION ALL


SELECT "der_id",
       "der_type",
       "type",
       "capacity_value",
       "capacity_unit"
FROM  dim_der


UNION ALL

SELECT  "transformer_id", 
        'grid_operations', 
        'power_transformer', 
        "capacity_mva", 
        'mva' 
FROM dim_power_transformer

UNION ALL

SELECT "transformer_id", 
       'grid_operations', 
       'distribution_transformer', 
       "rated_kva", 
       'kva' 

FROM dim_distribution_transformer;

