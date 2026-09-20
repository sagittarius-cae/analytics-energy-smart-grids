import duckdb


def get_duckdb_connection(database:str = "analytics.duckdb"):
    #  Connect to an in-memory DuckDB instance and stream the results directly
    connection = duckdb.connect(database)

    return connection
