# src/dagster_essentials/defs/assets/trips.py
import requests
from dagster_essentials.defs.assets import constants
from dagster_essentials.defs.partitions import monthly_partition
from dagster_duckdb import DuckDBResource
import dagster as dg
import pandas as pd


@dg.asset(
    partitions_def=monthly_partition,
    group_name="raw_files"
) # update the return type of the asset
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult: 
    """
    The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    # dynamically fetch a specific partition’s month of data
    partition_date_str = context.partition_key
    #Slice the string to make it match the format expected by our source system 'YYYY-MM-DD' -> 'YYYY-MM'
    month_to_fetch = partition_date_str[:-3]

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

    # Calculate the number of records contained in the file
    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))
    return dg.MaterializeResult(
        metadata={ # add the metadata with the specified type
            'Number of records': dg.MetadataValue.int(num_rows)
        }
    )


@dg.asset(
    group_name="raw_files"
)
def taxi_zones_file() -> dg.MaterializeResult:
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get(
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_taxi_zones.content)

    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

    return dg.MaterializeResult(
        metadata={
            'Number of records' : dg.MetadataValue.int(num_rows)

    })

@dg.asset(
  deps=["taxi_trips_file"],
  partitions_def=monthly_partition,
  group_name="ingested",
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
  """
    The raw taxi trips dataset, loaded into a DuckDB database, partitioned by month.
  """

  partition_date_str = context.partition_key
  month_to_fetch = partition_date_str[:-3]

  query = f"""
    CREATE TABLE IF NOT EXISTS trips (
      vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
      rate_code_id double, payment_type integer, dropoff_datetime timestamp,
      pickup_datetime timestamp, trip_distance double, passenger_count double,
      total_amount double, partition_date varchar
    );

    DELETE FROM trips WHERE partition_date = '{month_to_fetch}';

    INSERT INTO trips
    SELECT
      VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
      tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
    FROM '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
  """

  with database.get_connection() as conn:
      conn.execute(query)

@dg.asset(
    deps=["taxi_zones_file"],
    group_name="ingested",
)
def taxi_zones(database: DuckDBResource) -> None:
    query = f'''
        CREATE OR REPLACE TABLE zones AS (
            SELECT
                LocationID AS zone_id,
                zone,
                borough,
                the_geom as geometry
            FROM '{constants.TAXI_ZONES_FILE_PATH}'
        );
    '''

    with database.get_connection() as conn:
        conn.execute(query)
    