"""
Mobile phone data processing
Sheffield application

Author Fulvio D. Lopane
Centre for Advanced Spatial Analysis

started coding: October 2024
"""

import duckdb
import pandas as pd
import geopandas as gpd
import os

from config import *

# Extract the Sheffield OA trajectories from the EW dataset (if file not there already)
if not os.path.isfile(outputs["MobilePhoneData_Sheffield"]):
    print("Extracting Sheffield trajectories from the EW dataset...")

    file_path = inputs["MobilePhoneData_EW"]

    # Import Sheffield OA csv file
    Sheffield_OA_df = set(gpd.read_file(inputs["Sheffield_OA_geojson"]).OA21CD.tolist())
    OA_set_str = str(tuple(Sheffield_OA_df))

    query = f"""
    SELECT
        *
    FROM
        parquet_scan('{file_path}') as activities
    WHERE
        activities.o_oa IN {OA_set_str}
        OR activities.d_oa IN {OA_set_str}
    """

    result = duckdb.sql(query).df()

    print(result.columns)

    # result is a Pandas DataFrame, save it to a parquet file
    result.to_parquet(outputs["MobilePhoneData_Sheffield"], index=False)

