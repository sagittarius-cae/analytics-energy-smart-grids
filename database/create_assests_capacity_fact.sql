-- ============================================================
-- FACT 2: asset_capacity — snapshot de capacidad instalada,
-- unificando 5 medidas heterogéneas de forma segura (patrón EAV)
-- ============================================================
CREATE OR REPLACE VIEW fact_asset_capacity AS
SELECT "Plant Id" AS "Asset Id", 
       'Bulk Generation' AS "Domain", 
       'Power Plant' AS "Asset Type", 
       "Capacity Mw" AS "Value", 
       'MW' AS "Unit" 
FROM dim_power_plants

UNION ALL

SELECT "Source Id", 
       'DER', 
       'Renewable Source', 
       "Output Kw", 
       'kW' 
FROM renewable_source

UNION ALL

SELECT "Storage Id", 
       'DER', 
       'Energy Storage', 
       "Capacity Kwh", 
       'kWh' 
FROM energy_storage

UNION ALL

SELECT "Transformer Id", 
        'Grid Operations', 
        'Power Transformer', 
        "Capacity Mva", 
        'MVA' 
FROM dim_power_transformer

UNION ALL

SELECT "Transformer Id", 
       'Grid Operations', 
       'Distribution Transformer', 
       "Rated Kva", 
       'kVA' 

FROM dim_distribution_transformer;

Por qué agregu