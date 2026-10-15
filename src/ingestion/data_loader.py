import os
from datetime import datetime
from pathlib import Path

import polars as pl

# This file lives in <project>/src/ingestion/, so the project root is parents[2]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "1_rawdata"
BRONZE_DIR = PROJECT_ROOT / "data" / "2_bronze"


def ingest_data(filename: str) -> pl.LazyFrame:
    """Read a raw CSV, add ingestion metadata and persist it as bronze parquet."""
    ingestion_time = datetime.now()

    csv_path = RAW_DIR / f"{filename}.csv"
    parquet_path = BRONZE_DIR / f"{filename}.parquet"
    tmp_path = parquet_path.with_suffix(".parquet.tmp")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    lazy_df = pl.scan_csv(csv_path).with_columns(
        pl.lit(ingestion_time).alias("ingested_at"),
        pl.lit(f"{filename}.csv").alias("ingested_from"),
    )

    # Write to a temp file, then rename: readers never see a half-written parquet
    lazy_df.sink_parquet(tmp_path)
    os.replace(tmp_path, parquet_path)

    return pl.scan_parquet(parquet_path)


def read_parquet_data(filename: str) -> pl.LazyFrame:
    """Lazily read a table from the bronze layer."""
    return pl.scan_parquet(BRONZE_DIR / f"{filename}.parquet")