import duckdb
import polars as pl
import os

DB_PATH = "database/analytics.db"

def get_duckdb_connection():
    """Returns a connection to the persistent DuckDB file."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)

def parse_dates(df: pl.DataFrame, column_name: str, date_format: str = "%Y-%m-%d") -> pl.DataFrame:
    """Safely converts a string column into a Polars Date type."""
    return df.with_columns(pl.col(column_name).str.to_date(date_format, strict=False))
