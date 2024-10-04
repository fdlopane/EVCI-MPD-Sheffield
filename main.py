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
if not os.path.isfile(outputs["MobilePhoneData_Sheffield_csv"]):
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

    # result is a Pandas DataFrame, save it to a parquet file and csv
    result.to_parquet(outputs["MobilePhoneData_Sheffield_parquet"], index=False)
    result.to_csv(outputs["MobilePhoneData_Sheffield_csv"], index=False)

# Extract casual daily activities (no work or home activities) and count per OA:
if not os.path.isfile(outputs["Daily_casual_activities"]):
    file_path = outputs["MobilePhoneData_Sheffield_parquet"]

    query = f"""
            SELECT
                month(start_time) AS month,
                day(start_time) AS day,
                COUNT(*) AS activity_count,
                o_oa AS OA
            FROM
                parquet_scan('{file_path}') AS activities
            WHERE
                activities.activity_type = 'STATIONARY' 
                AND activities.home_activity = 'N'
                AND activities.work_activity = 'N'
                AND activities.activity_duration >= 5
            GROUP BY
                OA,month,day
            ORDER BY
                OA,month,day;
            """
    # Connect to DuckDB and execute the query
    with duckdb.connect() as conn:
        result = conn.execute(query).df()

    # save result to csv
    result.to_csv(outputs["Daily_casual_activities"], index=False)

