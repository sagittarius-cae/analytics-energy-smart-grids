# 🌟 PASO 1: LAS LÍNEAS 1, 2 Y 3 DEBEN SER ESTAS OBLIGATORIAMENTE
import sys
import os
sys.path.append("/workspace") 

# 🌟 PASO 2: AHORA SÍ, SE EJECUTAN LOS IMPORTS GENERALES DEL SISTEMA
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 🌟 PASO 3: IMPORTS GLOBALES DE TU LÓGICA DE NEGOCIO
from src.ingestion import data_loader
from src.transformation import data_cleanser
from src.modeling import semantic_views

# 1. Configuración explícita de tus archivos y sus argumentos específicos de limpieza
CONFIG_PIPELINE = {
    "utility_provider": {"id_col": "provider_id", "extra_args": None},
    "substation": {"id_col": "substation_id", "extra_args": ['voltage_kv']},
    "consumer": {"id_col": "consumer_id", "extra_args": None},
    "ami_head_end": {"id_col": "hes_id", "extra_args": None},
    "data_mgmt_system": {"id_col": "system_id", "extra_args": None},
    "distribution_network": {"id_col": "network_id", "extra_args": None},
    "distribution_transformer": {"id_col": "transformer_id", "extra_args": ['rated_kva']},
    "energy_storage": {"id_col": "storage_id", "extra_args": ['capacity_kwh']},
    "power_plants": {"id_col": "plant_id", "extra_args": ['capacity_mw']},
    "power_transformer": {"id_col": "transformer_id", "extra_args": ['capacity_mva']}, 
    "renewable_source": {"id_col": "source_id", "extra_args": ['output_kw']},
    "scada_dms": {"id_col": "system_id", "extra_args": None},
    "smart_meters": {"id_col": "meter_id", "extra_args": None}
}

# 2. LISTA ORDENADA DE ARCHIVOS SQL
SQL_FILES_ORDER = [
    "create_consumer_view.sql",
    "create_dim_utility_provider_view.sql", 
    "create_dim_data_mgmt_system_view.sql",
    "create_dim_generation_der_view.sql",
    "create_dim_metering_view.sql",
    "create_dim_generation_bulk_view.sql",  
    "create_dim_operation_grid_view.sql",
    "create_fact_smart_meters_view.sql",
    "create_fact_assets_capacity_view.sql"
]

# PASO 1 y 2 GRANULAR: Se ejecuta de forma independiente por cada archivo CSV
def run_data_processing(file_name, id_column, cleansing_arguments):
    df_raw = data_loader.ingest_data(file_name)
    data_cleanser.curate_data(df_raw, file_name, id_column, cleansing_arguments)
    return f"File {file_name} processed."

def run_semantic_modeling():
    analytics_repo = semantic_views.AnalyticsRepository()
    sql_base_dir = os.path.join("/workspace", "database")
    
    for sql_file in SQL_FILES_ORDER:
        full_sql_path = os.path.join(sql_base_dir, sql_file)
        analytics_repo.create_sql_views(full_sql_path)
    
    analytics_repo.materialize_views_to_parquet()
    analytics_repo.close_duckdb_connection()
    return "Capa semántica completa."


# Configuración estándar del DAG de Airflow
default_args = {
    'owner': 'smart_grids_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='smart_grids_analytics_pipeline',
    default_args=default_args,
    description='Pipeline de Smart Grids: Limpieza Granular + Modelado Semántico DuckDB',
    schedule_interval='@daily',
    catchup=False
) as dag:

    data_curation_tasks = [] 

    # 3. Construcción explícita de las tareas granulares
    for file_name, config in CONFIG_PIPELINE.items():
        task_id = f'process_{file_name}' 
        
        task_cleansing = PythonOperator(
            task_id=task_id,
            python_callable=run_data_processing,
            op_kwargs={
                'file_name': file_name,
                'id_column': config['id_col'],
                'cleansing_arguments': config['extra_args']
            }
        )
        data_curation_tasks.append(task_cleansing) 

    # Tarea unificada de modelado (DuckDB en memoria)
    modeling_task = PythonOperator(
        task_id='crear_capa_semantica_duckdb',
        python_callable=run_semantic_modeling
    )

    # 4. Orquestación y dependencias jerárquicas
    data_curation_tasks >> modeling_task
