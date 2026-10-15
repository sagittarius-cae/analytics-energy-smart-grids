# 🌟 PASO 1: LAS LÍNEAS 1, 2 Y 3 DEBEN SER ESTAS OBLIGATORIAMENTE
import sys
import os
from datetime import datetime, timedelta
sys.path.append("/workspace") 

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_PIPELINE = {
    "utility_provider": {"id_col": "provider_id", "extra_args": None},
    "substation": {"id_col": "substation_id", "extra_args": ["voltage_kv"]},
    "consumer": {"id_col": "consumer_id", "extra_args": None},
    "ami_head_end": {"id_col": "hes_id", "extra_args": None},
    "data_mgmt_system": {"id_col": "system_id", "extra_args": None},
    "distribution_network": {"id_col": "network_id", "extra_args": None},
    "distribution_transformer": {"id_col": "transformer_id", "extra_args": ["rated_kva"]},
    "energy_storage": {"id_col": "storage_id", "extra_args": ["capacity_kwh"]},
    "power_plants": {"id_col": "plant_id", "extra_args": ["capacity_mw"]},
    "power_transformer": {"id_col": "transformer_id", "extra_args": ["capacity_mva"]},
    "renewable_source": {"id_col": "source_id", "extra_args": ["output_kw"]},
    "scada_dms": {"id_col": "system_id", "extra_args": None},
    "smart_meters": {"id_col": "meter_id", "extra_args": None},
}

# Order matters: later views depend on earlier ones
SQL_FILES_ORDER = [
    "create_consumer_view.sql",
    "create_dim_utility_provider_view.sql",
    "create_dim_data_mgmt_system_view.sql",
    "create_dim_generation_der_view.sql",
    "create_dim_metering_view.sql",
    "create_dim_generation_bulk_view.sql",
    "create_dim_operation_grid_view.sql",
    "create_fact_smart_meters_view.sql",
    "create_fact_assets_capacity_view.sql",
]

SQL_BASE_DIR = os.path.join("/workspace", "database")

# ---------------------------------------------------------------------------
# Task callables
# Heavy imports (polars, duckdb via src.*) are kept inside the functions so the
# scheduler does not load them every time it parses this file.
# ---------------------------------------------------------------------------

def run_ingestion(file_name):
    """Stage 1: read the raw CSV and persist it as parquet in data/2_bronze."""
    from src.ingestion import data_loader

    data_loader.ingest_data(file_name)


def run_curation(file_name, id_column, cleansing_arguments):
    """Stage 2: read the bronze parquet lazily and apply the cleansing rules."""
    from src.ingestion import data_loader
    from src.transformation import data_cleanser

    df_raw = data_loader.read_parquet_data(file_name)
    data_cleanser.curate_data(df_raw, file_name, id_column, cleansing_arguments)


def run_semantic_modeling():
    """Stage 3: build the DuckDB views in order and materialize them to parquet."""
    from src.modeling import semantic_views

    analytics_repo = semantic_views.AnalyticsRepository()
    try:
        for sql_file in SQL_FILES_ORDER:
            analytics_repo.create_sql_views(os.path.join(SQL_BASE_DIR, sql_file))
        analytics_repo.materialize_views_to_parquet()
    finally:
        analytics_repo.close_duckdb_connection()

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

default_args = {
    "owner": "smart_grids_team",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id = "smart_grids_analytics_pipeline",
    default_args = default_args,
    description = "Smart Grids: Ingestion -> Curation -> Semantic Layer (DuckDB)",
    schedule_interval = "@daily",  # use schedule="@daily" if you are on Airflow 2.4+
    catchup = False,
    max_active_tasks = 4,          # limits parallel tasks to protect memory (smart_meters)
) as dag:

    ingest_tasks, curate_tasks = {}, {}

    with TaskGroup(group_id="ingestion") as ingestion:
        for name in CONFIG_PIPELINE:
            ingest_tasks[name] = PythonOperator(
                task_id=f"ingest_{name}",
                python_callable = run_ingestion,
                op_kwargs={"file_name": name},
            )

    with TaskGroup(group_id="curation") as curation:
        for name, cfg in CONFIG_PIPELINE.items():
            curate_tasks[name] = PythonOperator(
                task_id=f"curate_{name}",
                python_callable = run_curation,
                op_kwargs={
                    "file_name": name,
                    "id_column": cfg["id_col"],
                    "cleansing_arguments": cfg["extra_args"],
                },
            )
            # Each file is curated as soon as its own ingestion finishes
            ingest_tasks[name] >> curate_tasks[name]

    semantic_layer = PythonOperator(
        task_id="semantic_layer",
        python_callable=run_semantic_modeling,
    )

    # The semantic layer runs only when all 13 files are curated
    curation >> semantic_layer