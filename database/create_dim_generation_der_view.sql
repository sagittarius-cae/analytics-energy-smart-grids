-- ============================================================
-- 4. DER — CAMBIO: ahora es renewable_source + energy_storage
-- Output Kw (potencia) y Capacity Kwh (energía) NO son la misma
-- magnitud física — se quedan en columnas separadas a propósito,
-- no se fusionan en un solo "capacity" inventado.
-- ============================================================

CREATE OR REPLACE VIEW dim_der AS
SELECT
     source_id AS "der_id",
    'renewable_source' AS "der_type",
    "type",
    "output_kw" AS "capacity_value",
    'kW' AS "capacity_unit"
FROM read_parquet('/workspace/data/3_cleaned/renewable_source.parquet')

UNION ALL

SELECT
    storage_id AS "der_id",
    'Energy Storage' AS "der_type",
    "type",
    "capacity_kwh" AS "capacity_value",
    'kWh' AS "capacity_unit"
FROM read_parquet('/workspace/data/3_cleaned/energy_storage.parquet');