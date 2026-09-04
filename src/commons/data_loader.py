import os
from pathlib import Path
import polars as pl
from datetime import datetime


def ingest_data(filename:str)->pl.LazyFrame:
    
    ingestion_time = datetime.now()

    # Starts directly in your current notebook directory
    data_dir = Path.cwd() / "data/1_rawdata" if (Path.cwd() / "data").exists() else Path.cwd().parent / "data/1_rawdata"
    parquet_dir = Path.cwd() / "data/2_bronze" if (Path.cwd() / "data").exists() else Path.cwd().parent / "data/2_bronze"
    
    # Combine paths cleanly
    csv_path = data_dir / f"{filename}.csv"
    parquet_path = parquet_dir / f"{filename}.parquet"

    lazy_df = (
        # 1. Scan the CSV lazily
        pl.scan_csv(csv_path)

        # 2. Add metadata columns using lit() for literal values
        .with_columns(
           [
                pl.lit(ingestion_time).alias("ingested_at"),
                pl.lit(f"{filename}.csv").alias("ingested_from"),
           ]
        )   
    )
    
    #3. Write to parquet file
    lazy_df.sink_parquet(parquet_path)

    #return lazy_df.collect(streaming=True)
    return lazy_df



