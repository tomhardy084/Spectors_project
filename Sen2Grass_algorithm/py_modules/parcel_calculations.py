# import modules
import geopandas as gpd
import glob2
import numpy as np
import os
import pandas as pd
from rasterstats import zonal_stats


# define functions
def select_parcels(input_schema, engine, output_path, s2_date):
    """
    Function to select all grassland parcels in on s2 tile and 
    store them in geodataframe, and as a geojson in the data directory.
    Function argument(s):
    - input_schema: the input schema from the Nexus database
      where the tables are stored
    - engine: an engine to connect to a sql database
    - output_path: the image path where the sentinel-2 bands should be stored
    - s2_date: a date for which calculation were performed on s2 data
    """
    # define query to select all grassland parcels within
    # study area and store them in geodataframe (try for max. 500 parcels)
    parcel_query = (f"SELECT id, id_src, polygon, vegetation_type "
                    f"FROM {input_schema}.plant_cover "
                    f"WHERE vegetation_type = 'pasture' and "
                    f"end_date > date '{s2_date}' - INTERVAL '1 year' "
                    f"LIMIT 500")
    
    # store records in dataframe
    parcel_gdf = gpd.GeoDataFrame.from_postgis(parcel_query,
                                               engine,
                                               geom_col = "polygon")
    
    # write parcel dataframe to file and set parcels_present to True
    parcels_file = f"{output_path}/parcels.geojson"
    if not parcel_gdf.empty and not os.path.isfile(parcels_file):
        parcel_gdf.to_file(parcels_file, driver = "GeoJSON")
        parcels_present = True
        
    # set parcels_present to False if no parcels are present
    else:
        parcels_present = False
    
    # remove query
    parcel_query = None
    
    # return dataframe and boolean
    return parcel_gdf, parcels_present


def calc_zonal_stats(output_path, veg_indices, time_stamp):
    """
    Function to calculate zonal statistics for all grassland parcels within 
    one sentinel tile, including veg index mean and std. and cloud cover %
    and store them in a pandas data frame.
    Function arguments:
    - output_path: the image path where the sentinel-2 bands are stored
    - veg_indices: a list with vegetation indices (NDVI, WDVI,
                                                   NDRE, CI_Red_Edge)
    - time_stamp: date and time for which calculations were performed
      (in <YYYY><MM><DD> format)
    """
    # initiate loop to iterate over all four indices
    for i in range(len(veg_indices)):
        
        # set input files
        vector_file = f"{output_path}/parcels.geojson"
        raster_file = glob2.glob(f"{output_path}/*{time_stamp}*"
                                 f"{veg_indices[i]}*.tif")[0]
        
        # calculate zonal statistics
        stats = zonal_stats(vectors = vector_file,
                            raster = raster_file,
                            nodata = np.nan,
                            stats = ["mean", "std", "count", "nan"],
                            all_touched = False)
        
        # store data in dataframe
        stats_df = pd.DataFrame(stats)
        mean_std = stats_df[["mean", "std"]]
        mean_std.columns = [f"{veg_indices[i]}_mean",
                            f"{veg_indices[i]}_std"]
        
        # calculate cloud cover percentage and concatenate to main df
        if i == 0:
            cloud_cover = pd.DataFrame(stats_df["nan"] / 
            (stats_df["count"] + stats_df["nan"]) * 100)
            cloud_cover.columns = ["cloud_cover_perc"]
            parcel_stats = pd.concat([cloud_cover, mean_std], axis = 1)
        
        # concatenate dataframes together
        else:
            parcel_stats = pd.concat([parcel_stats, mean_std], axis = 1)
        
        # remove variables
        stats, mean_std, cloud_cover = None, None, None
        
    # return the dataframe
    return parcel_stats


def upload_parcels_df(time_stamp, tile_id, cloud_cover_perc, parcels,
                      parcel_stats, output_table, engine, output_schema):
    """
    Function to upload and append new parcels dataframe containing
    zonal statistics calculations to Nexus.
    Function argument(s):
    - time_stamp: a tile stamp value retrieved from table at Nexus server
    - tile_id: a tile id value retrieved from table at Nexus server
    - cloud_cover_perc: the maximum chosen cloud cover percentage
    - parcels: the original parcels dataframe retrieved from Nexus server
    - parcel_stats: the calculated parcels statistics in a dataframe
    - output_table: the name of the output table to upload to Nexus
    - engine: an engine to connect to a sql database
    - output_schema: the output schema from the Nexus database
    """
    # create time_stamp series
    time_stamp_col = pd.DataFrame([time_stamp] * parcels.shape[0])
    time_stamp_col.columns = ["time_stamp"]
    
    # create tile_id series
    tile_id_col = pd.DataFrame([tile_id] * parcels.shape[0])
    tile_id_col.columns = ["tile_id"]
    
    # concatenate all dataframes into one large dataframe
    parcels_new_df = pd.concat([parcels["id"],
                                parcels["id_src"],
                                tile_id_col,
                                time_stamp_col,
                                parcels["vegetation_type"],
                                parcel_stats], axis = 1)
    
    # remove records that contain more cloud cover than given percentage
    parcels_new_df = parcels_new_df.drop(parcels_new_df[
    parcels_new_df["cloud_cover_perc"] >= cloud_cover_perc].index)
    
    # if dataframe contains information, upload it to Nexus server
    if not parcels_new_df.empty:
        parcels_new_df.to_sql(output_table, engine,
                              schema = output_schema,
                              if_exists = "append")
    
    # remove variables
    time_stamp_col, tile_id_col = None, None
    
    # return new parcel dataframe
    return parcels_new_df