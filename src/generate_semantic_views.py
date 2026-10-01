import os
import duckdb
import polars as pl


class AnalyticsRepository:

    def __init__(self):
       
        # 1. Locate the folder 'database/' (sibbling of'src/')
        src_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_dir = os.path.abspath(os.path.join(src_dir, "..", "database"))

        
        # 2. Define the path for the database file.
        self.db_path = os.path.join(self.db_dir, "energy_analytics_prod.duckdb")

        # 3. Establish the duckdb engine connection in memory.
        self.duckdb_connection = duckdb.connect(self.db_path)

    
    def create_sql_views(self, sql_file:str):

        db_dir = self.db_dir

        # --- VIEW 1: Consumer ---
        path_consumer_view = os.path.join(self.db_dir, sql_file)

        with open(path_consumer_view, "r", encoding="utf-8") as f:
            self.duckdb_connection.execute(f.read())
            print(f"  ✅  View created from {sql_file} file.")
        

    def select_dim_consumer(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_consumer').pl().lazy()
        return lazy_frame
    
    def select_dim_utility_provider(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_utility_provider').pl().lazy()
        return lazy_frame

    
    def select_dim_data_mgmt_system(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_data_mgmt_system').pl().lazy()
        return lazy_frame


    def select_dim_generation_der(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_der').pl().lazy()
        return lazy_frame
    
    def select_dim_metering(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_metering').pl().lazy()
        return lazy_frame
    
    def select_dim_generation_bulk(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_generation_bulk').pl().lazy()
        return lazy_frame

    def select_dim_operation_grid(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_operation_grid').pl().lazy()
        return lazy_frame

    def select_fact_smart_meters(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM fact_smart_meters').pl().lazy()
        return lazy_frame

    def select_fact_assets_capacity(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM fact_assets_capacity').pl().lazy()
        return lazy_frame


    
    
    
    def close_duckdb_connection(self):
        self.duckdb_connection.close()
