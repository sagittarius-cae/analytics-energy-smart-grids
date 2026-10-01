-- ============================================================
-- 5. Grid Operations
-- ============================================================

CREATE OR REPLACE VIEW dim_power_transformer AS
SELECT transformer_id,
       substation_id,
       capacity_mva
FROM read_parquet('/workspace/data/3_cleaned/power_transformer.parquet');

CREATE OR REPLACE VIEW dim_distribution_transformer AS
SELECT transformer_id, 
       network_id, 
       rated_kva
FROM read_parquet('/workspace/data/3_cleaned/distribution_transformer.parquet');


CREATE OR REPLACE VIEW dim_distribution_network AS
SELECT dn.network_id, 
       dn.substation_id, 
       dn.feeder_type, dn.scada_id,
       (SELECT COUNT(*) FROM dim_distribution_transformer dt WHERE dt.network_id = dn.network_id) AS "distribution_transformer_count"
FROM read_parquet('/workspace/data/3_cleaned/distribution_network.parquet') dn;


CREATE OR REPLACE VIEW dim_operation_grid AS
SELECT
    s.substation_id,
    s.substation_type, 
    s.location, 
    s.voltage_kv,
    s.source_type, 
    s.source_id,
    sc.system_id AS "scada_system_id", 
    sc.function AS "scada_function", 
    sc.control_center,
    p.provider_id, 
    p.name AS "provider_name", 
    p.region AS "provider_region",
    CASE s.source_type WHEN 'POWER_PLANT' THEN bg.type ELSE der.type END AS "source_asset_type",
    (SELECT COUNT(*) FROM dim_power_transformer pt WHERE pt.substation_id = s.substation_id) AS "power_transformer_count",
    (SELECT COUNT(*) FROM dim_distribution_network dn WHERE dn.substation_id = s.substation_id) AS "distribution_network_count"
FROM read_parquet('/workspace/data/3_cleaned/substation.parquet') s
LEFT JOIN read_parquet('/workspace/data/3_cleaned/scada_dms.parquet') sc   ON sc.system_id = s.scada_id
LEFT JOIN dim_utility_provider p   ON p.provider_id = sc.provider_id
LEFT JOIN dim_generation_bulk bg ON s.source_type = 'POWER_PLANT' AND bg.plant_id = s.source_id
LEFT JOIN dim_der der            ON s.source_type IN ('RENEWABLE_SOURCE','ENERGY_STORAGE') AND der.der_id = s.source_id;

