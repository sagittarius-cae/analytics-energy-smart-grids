from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# Imports de tu lógica de negocio modular en src/
from src.ingestion.data_loader import data_loader
from src.transformation.data_cleanser import data_cleanser
from src.modeling.analytics_repo import AnalyticsRepository
from src.visualization.dashboard import generate_seaborn_charts

# 1. Configuración explícita de tus 15 archivos y sus argumentos específicos de limpieza

CONFIG_PIPELINE = {
    "utility_provider": {
        "id_col": "provider_id",
        "extra_args": None # Argumentos únicos
    },
    "substation": {
        "id_col": "substation_id",
        "extra_args":  ['voltage_kv']
    },
    "consumer": {
        "id_col": "consumer_id",
        "extra_args": None # Sin argumentos opcionales
    },
    "ami_head_end": {
        "id_col": "hes_id",
        "extra_args": None # Sin argumentos opcionales
    },
    "data_mgmt_system": {
        "id_col": "system_id",
        "extra_args": None # Sin argumentos opcionales
    },
    "distribution_network": {
        "id_col": "network_id",
        "extra_args": None # Sin argumentos opcionales
    },
    "distribution_transformer": {
        "id_col": "transformer_id",
        "extra_args": ['rated_kva'] # Sin argumentos opcionales
    },
    "energy_storage": {
        "id_col": "storage_id",
        "extra_args":  ['capacity_kwh'] # Sin argumentos opcionales
    },
    "power_plants": {
        "id_col": "plant_id",
        "extra_args": ['capacity_mw'] # Sin argumentos opcionales
    },
    "power_transformer": {
        "id_col": "transfomer",
        "extra_args": ['capacity_mva'] # Sin argumentos opcionales
    },
    "renewable_source": {
        "id_col": "source_id",
        "extra_args": ['output_kw'] # Sin argumentos opcionales
    },
    "scada_dms": {
        "id_col": "system_id",
        "extra_args": None # Sin argumentos opcionales
    },
    "smart_meters": {
        "id_col": "meter_id",
        "extra_args": None # Sin argumentos opcionales
    },
    # ... Registra aquí tus 12 archivos restantes con sus respectivas configuraciones
}

# PASO 1 y 2 GRANULAR: Se ejecuta de forma independiente por cada archivo CSV
def run_data_processing(file_name, id_column, cleansing_arguments):
    # Ingestión (Bronze - Retorna LazyFrame)
    df_raw = data_loader.ingest_data(file_name)
    
    # Limpieza (Silver - Procesa en memoria y persiste el Parquet curado en la carpeta data/)
    data_cleanser.clean_data(df_raw, file_name, id_column, cleansing_arguments)
    
    return f"File {file_name} processed and as curated parquet."

# PASO 3 UNIFICADO: Ejecuta DuckDB en memoria leyendo los Parquet curados
def run_semantic_modeling():
    analytics_repo = AnalyticsRepository()
    
    # Definimos la ruta exacta dentro del contenedor hacia tu carpeta database/
    sql_file_path = os.path.join("/workspace", "database", "create_consumer_view.sql")
    
    # DuckDB enciende en memoria, lee los Parquet curados desde 'data/', ejecuta el SQL y persiste la Fase Gold
    analytics_repo.create_sql_views(sql_file_path)
    df_gold = analytics_repo.select_dim_consumer()
    
    return "Modelo semántico DuckDB finalizado y guardado exitosamente."

# PASO 4: Visualización
def run_dashboard_generation():
    generate_seaborn_charts()
    return "Dashboard de Smart Grids generado exitosamente."


# 2. Configuración estándar del DAG de Airflow
default_args = {
    'owner': 'smart_grids_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='smart_grids_analytics_pipeline',
    default_args=default_args,
    description='Pipeline de Smart Grids: Limpieza Granular + Modelado Semántico DuckDB + Seaborn',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Lista para almacenar las 15 tareas paralelas de limpieza
    limpieza_tasks = []

    # 3. Construcción explícita de las tareas granulares
    for file_name, config in CONFIG_PIPELINE.items():
        
        task_id = f'procesar_{file_name}'
        
        task_limpieza = PythonOperator(
            task_id=task_id,
            python_callable=run_data_processing,
            op_kwargs={
                'file_name': file_name,
                'id_column': config['id_col'],
                'cleansing_arguments': config['extra_args']
            }
        )
        limpieza_tasks.append(task_limpieza)

    # Tarea unificada de modelado (DuckDB en memoria)
    modeling_task = PythonOperator(
        task_id='crear_capa_semantica_duckdb',
        python_callable=run_semantic_modeling
    )

    # Tarea de reportería final (Seaborn)
    reporting_task = PythonOperator(
        task_id='generar_reporte_seaborn',
        python_callable=run_dashboard_generation
    )

    # 4. Orquestación y dependencias jerárquicas
    # Las 15 tareas corren en paralelo. Cuando todas terminen bien, se ejecuta DuckDB y luego Seaborn.
    limpieza_tasks >> modeling_task >> reporting_task
