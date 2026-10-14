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

    ### - Create SQL views from the specific sql file as parameter
    def create_sql_views(self, sql_file:str):
        db_dir = self.db_dir
        path_view = os.path.join(self.db_dir, sql_file)

        with open(path_view, "r", encoding="utf-8") as f:
            self.duckdb_connection.execute(f.read())
            print(f"  ✅  View created from {sql_file} file.")
        
    ### -- DIM View Consumer
    def select_dim_consumer(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_consumer').pl().lazy()
        return lazy_frame

    ### -- DIM View Utility Provider
    def select_dim_utility_provider(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_utility_provider').pl().lazy()
        return lazy_frame

    ### -- DIM View Data Management System
    def select_dim_data_mgmt_system(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_data_mgmt_system').pl().lazy()
        return lazy_frame

    ### -  Dim View Generation DER
    def select_dim_generation_der(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_der').pl().lazy()
        return lazy_frame
    
    ### - Dim View Metering
    def select_dim_metering(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_metering').pl().lazy()
        return lazy_frame
    
    ### - Dim View Generation Bulk
    def select_dim_generation_bulk(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_generation_bulk').pl().lazy()
        return lazy_frame

    ### - Dim View Operations Grid
    def select_dim_operation_grid(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM dim_operation_grid').pl().lazy()
        return lazy_frame
    
    ### - Fact View Smart Meters
    def select_fact_smart_meters(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM fact_smart_meters').pl().lazy()
        return lazy_frame

     ### - Fact View Assets Capacity
    def select_fact_assets_capacity(self)->pl.LazyFrame:
        lazy_frame = self.duckdb_connection.execute('SELECT * FROM fact_assets_capacity').pl().lazy()
        return lazy_frame

    ### - Generate parquet files with the correct semantic views to be plotted on dashboards after.
    def materialize_views_to_parquet(self, output_dir="/workspace/data/4_analytics"):
        # Ensure the target analytics directory exists
        os.makedirs(output_dir, exist_ok=True)

        # List of all semantic dimensions and fact views to materialize
        views_to_export = [
            "dim_utility_provider",
            "dim_consumer",
            "dim_data_mgmt_system",
            "dim_der",
            "dim_generation_bulk",
            "dim_power_transformer",
            "dim_distribution_transformer",
            "dim_distribution_network",
            "dim_metering",
            "dim_operation_grid",  # Pillar 2
            "fact_assets_capacity",  # Pillar 1[cite: 1]
            "fact_smart_meters",  # Pillars 3 & 4[cite: 4]
        ]   
        print("Starting materialization of semantic layer to Parquet...")

        for view_name in views_to_export:
            file_path = os.path.join(output_dir, f"{view_name}.parquet")
            # Execute DuckDB's COPY command to write the view to disk
            self.duckdb_connection.execute(
                f"COPY (SELECT * FROM {view_name}) TO '{file_path}' (FORMAT PARQUET);"
            )
            print(f"  [EXPORTED] {view_name} -> {file_path}")

        print("Materialization complete!")
    
    ### -  Close duckdb connection from the current instance.
    def close_duckdb_connection(self):
        self.duckdb_connection.close()

    
    ### -- Scan Analytics Layer
    
    def scan_analytics_layer(self)-> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
        analytics_dir: str = "/workspace/data/4_analytics"

        """Returns lazy scans of core analytics parquet files to protect RAM."""
        return {
            "capacity": pl.scan_parquet(f"{analytics_dir}/fact_assets_capacity.parquet"),
            "grid": pl.scan_parquet(f"{analytics_dir}/dim_operation_grid.parquet"),
            "meters": pl.scan_parquet(f"{analytics_dir}/fact_smart_meters.parquet"),
        }
