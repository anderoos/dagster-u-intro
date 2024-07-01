import requests
from dagster_duckdb import DuckDBResource
from . import constants
from dagster import asset, AssetExecutionContext  # Provides metadata about materialization
import duckdb
import os

from ..partitions import monthly_partition


@asset(
    partitions_def=monthly_partition
)
def taxi_trips_file(context: AssetExecutionContext) -> None:
    """
        The raw parquet files for the taxi trips dataset. Sourced from NYCOpen Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]  # Extract correct formatting
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)


@asset
def taxi_zones_file() -> None:
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_taxi_zones = requests.get(
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_taxi_zones.content)


@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition
)
def taxi_trips(context: AssetExecutionContext, database: DuckDBResource) -> None:
    """
      Ingests partitioned taxi_trips_file data and inserts to 'trips' table using three queries.
        1. Create trips table.
        2. Delete previously inserted data.
        3. Insert data into trips.
    """

    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    create_table_query = f"""
        CREATE OR REPLACE TABLE trips (
            vendor_id INTEGER, 
            pickup_zone_id INTEGER, 
            dropoff_zone_id INTEGER,
            rate_code_id DOUBLE, 
            payment_type INTEGER, 
            dropoff_datetime TIMESTAMP,
            pickup_datetime TIMESTAMP, 
            trip_distance DOUBLE, 
            passenger_count DOUBLE,
            total_amount DOUBLE, 
            partition_date VARCHAR(20)
        );
    """

    delete_data_query = f"DELETE FROM trips WHERE partition_date = '{month_to_fetch}';"

    insert_data_query = f"""
            INSERT INTO trips (
                vendor_id, 
                pickup_zone_id, 
                dropoff_zone_id,
                rate_code_id, 
                payment_type, 
                dropoff_datetime,
                pickup_datetime, 
                trip_distance, 
                passenger_count,
                total_amount, 
                partition_date
            )
            SELECT
                VendorID,
                PULocationID,
                DOLocationID,
                RatecodeID,
                payment_type,
                tpep_dropoff_datetime,
                tpep_pickup_datetime,
                trip_distance,
                passenger_count,
                total_amount,
                {month_to_fetch} AS partition_date
            FROM '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
        """

    with database.get_connection() as conn:
        conn.execute(create_table_query)
        conn.execute(delete_data_query)
        conn.execute(insert_data_query)



@asset(
    deps=['taxi_zones_file']
)
def taxi_zones(database: DuckDBResource) -> None:
    """
        Loads taxi_zones_file into duckDB database.
    """
    sql_query = f"""CREATE OR REPLACE TABLE zones AS (
                    SELECT
                        LocationID AS zone_id,
                        zone,
                        borough,
                        the_geom AS geometry
                    FROM '{constants.TAXI_ZONES_FILE_PATH}'
                    );
                """
    with database.get_connection() as conn:
        conn.execute(sql_query)
