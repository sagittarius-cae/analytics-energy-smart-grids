-- ============================================================
-- FACT 2: asset_capacity — snapshot de capacidad instalada,
-- unificando 5 medidas heterogéneas de forma segura (patrón EAV)
-- ============================================================

CREATE OR REPLACE VIEW fact_assets_capacity AS

SELECT  CAST(der_id AS VARCHAR) AS "asset_id",
        CAST(der_type AS VARCHAR) AS "asset_type",
        CAST('Generation DER' AS VARCHAR) AS "domain",
        CAST(capacity_value AS DOUBLE) AS "value",
        CAST(capacity_unit AS VARCHAR) AS "unit"
FROM  dim_der


UNION ALL

SELECT  CAST(transformer_id AS VARCHAR) AS "asset_id", 
        CAST('POWER_TRANSFORMER' AS VARCHAR) AS "asset_type",
        CAST('Power Transformer' AS VARCHAR) as "domain",
        CAST(capacity_mva AS DOUBLE) AS "value", 
        CAST('mva' AS VARCHAR) AS "unit"
FROM dim_power_transformer

UNION ALL

SELECT  CAST(transformer_id AS VARCHAR) AS "asset_id",  
        CAST('DISTRIBUTION_TRANSFORMER'AS VARCHAR) AS "asset_type", 
        CAST('Distribution Transformer' AS VARCHAR) AS "domain",
        CAST(rated_kva AS DOUBLE) AS "value", 
        CAST('kva' AS VARCHAR) AS "unit" 

FROM dim_distribution_transformer

UNION ALL

SELECT  CAST(plant_id AS VARCHAr) AS "asset_id", 
        CAST('POWER_PLANT' AS VARCHAR) AS "asset_type", 
        CAST('Power Plants' AS VARCHAR) AS "domain",
        CAST(capacity_mw AS DOUBLE) AS "value", 
        CAST('mw' AS VARCHAR) AS "unit" 
FROM read_parquet('/workspace/data/3_cleaned/power_plants.parquet');

