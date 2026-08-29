import os
from pathlib import Path
import polars as pl
from datetime import datetime


def injest_data(filename:str)->pl.LazyDataFrame:
    
    ingestion_time = datetime.now()

    # Starts directly in your current notebook directory
    data_dir = Path.cwd() / "data/1_rawdata" if (Path.cwd() / "data").exists() else Path.cwd().parent / "data/1_rawdata"
    parquet_dir = Path.cwd() / "data/2_bronze" if (Path.cwd() / "data").exists() else Path.cwd().parent / "data/2_bronze"
    
    # Combine paths cleanly
    csv_path = data_dir / filename.".csv"

    parquet_path = parquet_dir / filename.".parquet"

    # 1. Scan the CSV lazily
    pl.scan_csv(filename)
    # 2. Add metadata columns using lit() for literal values
    .with_columns(
        [
            pl.lit("{source_file).cs").alias("ingestion_source"),
            pl.lit(ingestion_time).alias("ingestion_timestamp"),
        ]
    )
    # 3. Process via streaming engine to handle metadata creation
    .collect(streaming=True)
    # 4. Write the final DataFrame to Parquet
    .write_parquet(parquet_path)
)

def read_csv(filename:str)->pl.DataFrame:
    # Starts directly in your current notebook directory
    data_dir = Path.cwd() / "data/1_rawdata" if (Path.cwd() / "data").exists() else Path.cwd().parent / "data/1_rawdata"

    # Combine paths cleanly
    csv_path = data_dir / filename

    df = pl.read_csv(
        csv_path,
        has_header=True,
        separator=",",
    
        # Performance & Memory Tuning
        infer_schema_length=10000,  # Look at 10k rows to accurately detect data types
        n_rows=None,                # Set to an integer (e.g., 10000) to test a small subset first
    
        # Data Cleaning & Safety
        ignore_errors=False,        # Set to True to drop malformed/corrupted rows instead of crashing
        try_parse_dates=True        # Automatically convert date/time columns to Polars Date/Datetime types
    )

    # 2. Delete the column indexes if Polars read it without name
    # (Sometime the system exports an invisible index at the beginning that it does read as "column_0" or "column_1")
    corrupted_columns= [col for col in df.columns if "column" in col]
    if corrupted_columns:
        df = df.drop(corrupted_columns)

    return df


def parse_dates(df: pl.DataFrame, column_name: str, date_format: str = "%Y-%m-%d") -> pl.DataFrame:
    """Safely converts a string column into a Polars Date type."""
    return df.with_columns(pl.col(column_name).str.to_date(date_format, strict=False))

    