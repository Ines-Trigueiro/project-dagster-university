import dagster as dg

import matplotlib.pyplot as plt
import geopandas as gpd

import duckdb
import os

from dagster_essentials.defs.assets import constants



@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats() -> None:

