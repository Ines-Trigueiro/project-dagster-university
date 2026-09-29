import dagster as dg
from dagster_duckdb import DuckDBResource


database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE")      
)

# tells Dagster how to map the resources to specific key names. 
# In this case the database_resource resource just defined is mapped to the key name database
@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={"database": database_resource})
