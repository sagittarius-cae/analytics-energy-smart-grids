-- ============================================================
-- FACT 1: smart_meter — el único grano transaccional real (ver nota)
-- ============================================================

CREATE OR REPLACE VIEW fact_smart_meters AS
SELECT
    m."meter_id", 
    m."install_date", 
    m."reading_date", 
    m."comm_protocol",
    c."consumer_id", 
    c."account_type",
    dt."transformer_id" AS "distribution_transformer_id", 
    dt."rated_kva",
    dn."network_id", 
    dn."feeder_type",
    s."substation_id",
    s."substation_type", 
    s."location" AS "substation_location",
    s."provider_name" AS "grid_provider_name",
    h."hes_id", 
    h."network_type" AS "hes_network_type", 
    h."provider_name" AS "metering_provider_name"
FROM read_parquet('/workspace/data/3_cleaned/smart_meters.parquet') m
LEFT JOIN dim_consumer c                  ON c."consumer_id" = m."consumer_id"
LEFT JOIN dim_distribution_transformer dt ON dt."transformer_id" = m."transformer_id"
LEFT JOIN dim_distribution_network dn     ON dn."network_id" = dt."network_id"
LEFT JOIN dim_operation_grid s   ON s."substation_id" = dn."substation_id"
LEFT JOIN dim_metering h   ON h."hes_id" = m."hes_id";