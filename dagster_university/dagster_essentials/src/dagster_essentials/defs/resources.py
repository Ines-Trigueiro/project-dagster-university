import dagster as dg
from dagster_duckdb import DuckDBResource


database_resource = DuckDBResource(
    database="data/staging/data.duckdb"
)
