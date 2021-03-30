# import modules
import glob2
import pandas as pd
import os
from osgeo import gdal


# define function(s)
def download_s2_bands(config, input_schema, engine, s3, band_names,
                      output_path, s2_date, tile_geom):
    """
    Function to download sentinel-2 bands and store them in data directory.
    Function argument(s):
    - config: a configuration file
    - input_schema: the input schema from the Nexus database
      where the tables are stored
    - engine: an engine to connect to a sql database
    - s3: variable for connection with boto3 client
    - band_names: the names of the bands in a list ("scene_class",
                                                    "surf_refl_665nm",
                                                    "surf_refl_705nm",
                                                    "surf_refl_783nm",
                                                    "surf_refl_865nm")
    - output_path: the image path where the sentinel-2 bands should be stored
    - s2_date: the date for which sentinel-2 bands should be downloaded
    - tile_geom: a polygon geometry string retrieved from table at Nexus server
    """
    # define and run first query to extract initial tile id and date
    tile_query = (f"SELECT id, timestamp "
                  f"FROM {input_schema}.raster "
                  f"WHERE parameter LIKE '%%{band_names[0]}%%' and "
                  f"timestamp::date = '{s2_date}' and "
                  f"polygon = '{tile_geom}'")
    
    # store records in dataframe
    tile_df = pd.read_sql_query(tile_query, engine)
    
    # extract tile id source and timestamp
    if not tile_df.empty:
        tile_id = tile_df.iloc[0]["id"]
        time_stamp = str(tile_df.iloc[0]["timestamp"]).split(" ")[0]
        time_stamp = (time_stamp.replace("-", ""))
        
        # initiate loop to download selected bands
        for i in range(len(band_names)):
            
            # define and run second query to extract selected bands
            bands_query = (f"SELECT id, parameter "
                           f"FROM {input_schema}.raster "
                           f"WHERE id <= {tile_id} and "
                           f"id >= {tile_id - 11} and "
                           f"parameter LIKE '%%{band_names[i]}%%' and "
                           f"timestamp::date = '{s2_date}'")
            
            # store records in dataframe
            bands_df = pd.read_sql_query(bands_query, engine)
            
            # set filename and parameter name
            file_name = f"{str(bands_df.iloc[0]['id'])}"
            param_name = f"{str(bands_df.iloc[0]['parameter'])}"
                
            # set input and output filenames
            input_file = f"{input_schema}/raster/{file_name}.tif"
            output_file = f"{output_path}/{param_name}_{s2_date}.tif"
            
            # download data
            if not os.path.isfile(output_file):
                s3.download_file(config.BUCKET, input_file, output_file)
                        
    # else, set tile id and time_stamp to None
    else:
        tile_id, time_stamp = None, None
    
    # remove variables
    tile_query, tile_df, bands_query, bands_df = None, None, None, None
    
    # return variables
    return tile_id, time_stamp


def resample_s2_bands(output_path, band_names):
    """
    Function to resample the downloaded Sentinel-2 bands to 10m.
    Function argument(s):
    - output_path: the image path where the sentinel-2 bands are stored
    - band_names: the names of the bands in a list ("scene_class",
                                                    "surf_refl_665nm",
                                                    "surf_refl_705nm",
                                                    "surf_refl_783nm",
                                                    "surf_refl_865nm")
    """
    # initiate x, y resolution, width and height
    band_xRes, band_yRes = 10, 10
    band_width, band_height = 0, 0
    
    # initiate loop to perform the resampling
    for i in range(len(band_names)):
        
        # set input and output files
        input_file = glob2.glob(os.path.join(output_path,
                                             f"*{band_names[i]}*.tif"))[0]
        output_file = f"{input_file.split('.tif')[0]}_10m.tif"
        
        # set resampling method
        if "scene_class" in input_file:
            resample = "near"
        else:
            resample = "bilinear"
        
        # resample with gdal.Warp
        if not os.path.isfile(output_file):
            gdal.Warp(destNameOrDestDS = output_file,
                      srcDSOrSrcDSTab = input_file,
                      format = "GTiff",
                      xRes = band_xRes, yRes = band_yRes,
                      width = band_width, height = band_height,
                      resampleAlg = resample,
                      outputType = gdal.GDT_Float32)
        
        # reset x, y, width and height values
        if i == 0:
            band_xRes, band_yRes = None, None
            dims = gdal.Info(glob2.glob(output_file)[0],
                             format = "json")["size"]
            band_width, band_height = dims[0], dims[1]