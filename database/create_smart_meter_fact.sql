-- ============================================================
-- FACT 1: smart_meter — el único grano transaccional real (ver nota)
-- ============================================================

CREATE OR REPLACE VIEW fact_smart_meter AS
SELECT
    m."Meter Id", 
    m."Install Date", 
    m."Reading Date", 
    m."Comm Protocol",
    c."Consumer Id", 
    c."Account Type",
    dt."Transformer Id" AS "Distribution Transformer Id", 
    dt."Rated Kva",
    dn."Network Id", 
    dn."Feeder Type",
    s."Substation Id",
    s."Substation Type", 
    s."Location" AS "Substation Location",
    s."Provider Name" AS "Grid Provider Name",
    h."Hes Id", 
    h."Network Type" AS "Hes Network Type", 
    h."Provider Name" AS "Metering Provider Name"
FROM smart_meter m
LEFT JOIN dim_consumer c                  ON c."Consumer Id" = m."Consumer Id"
LEFT JOIN dim_distribution_transformer dt ON dt."Transformer Id" = m."Transformer Id"
LEFT JOIN dim_distribution_network dn     ON dn."Network Id" = dt."Network Id"
LEFT JOIN dim_substation s            ON s."Substation Id" = dn."Substation Id"
LEFT JOIN dim_ami_head_end h          ON h."Hes Id" = m."Hes Id";