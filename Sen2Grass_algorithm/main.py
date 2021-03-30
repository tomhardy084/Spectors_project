# import global modules
from datetime import date, timedelta
import boto3
import fire
import glob2
import logging
import os
import pandas as pd
import sqlalchemy
import sys


# import local modules
import config

from py_modules.create_directories import create_directories

from py_modules.s2_processing import(download_s2_bands,
                                     resample_s2_bands)

from py_modules.raster_calculations import (calc_veg_indices,
                                            mosaic_veg_indices,
                                            upload_veg_indices)

from py_modules.parcel_calculations import(select_parcels,
                                           calc_zonal_stats,
                                           upload_parcels_df)


# define main function to harvest, calculate and upload
# sentinel-2 data and information from and to Nexus
def run_task(input_schema, output_schema, output_table, calc_parcel_stats):
    """ 
    This function is created to connect to the Nexus geodatabase,
    harvest and process Sentinel-2 bands, calculate vegetation indices,
    calculate grassland parcel statistics for those indices, and
    upload calculations back to Nexus
    Function argument(s):
    - input_schema: the input schema from the Nexus database
    - output_schema: the output schema from the Nexus database
    - output_table: the name of the output table to upload to Nexus
    - calc_parcel_stats: boolean to able or disable parcel stats calculations
    """
    ### INITIALIZATION ###
    # initiate logging settings
    logging.basicConfig(stream = sys.stdout, level = logging.INFO,
                        format = ("%(asctime)s - %(name)s - "
                                  "%(levelname)s - %(message)s"))
    
    # connect to the Nexus Database
    logging.info("Starting the workfow and connecting to Nexus...")
    db = config.DATABASE
    url = (f"postgresql://{db['USER']}:{db['PASSWORD']}@{db['HOST']}:"
           f"{db['PORT']}/{db['NAME']}")
    engine = sqlalchemy.create_engine(url, client_encoding = "utf8",
                                      pool_size = 6)
    
    # connect to boto3 client
    s3 = boto3.client("s3", aws_access_key_id = config.ACCESS_KEY_ID,
                      aws_secret_access_key = config.SECRET_ACCESS_KEY)
    logging.info(f"Connected to the Nexus Database; "
                 f"calculation initialized with input schema "
                 f"{input_schema} and output_schema {output_schema}")
    
    # enter start and enddate here in YYYY-MM-DD format or
    # datetime.today instance for which S2 data should be processed
    start_date = date.today() - timedelta(days = 2)
    end_date = date.today() - timedelta(days = 0)
    
    # enter maximum cloud cover percentage
    cloud_cover_perc = 90
    
    # create range of dates
    time_range = pd.date_range(str(start_date), str(end_date))
    time_range = pd.Series(time_range).dt.date
    
    # store s2 band names in list
    band_names = ["scene_class", "surf_refl_665nm", "surf_refl_705nm",
                  "surf_refl_783nm", "surf_refl_865nm"]
    
    # store vegetation indices in list
    veg_indices = ["ndvi", "wdvi", "ndre", "ci_red_edge"]
    
    
    ### EXTRACTING TILE GEOMETRIES ###
    # create data and py modules directories and set data output path
    create_directories()
    output_path = "./data"
    
    # select all distinct tile geometries (id = 48 and s2 band = 665nm)
    logging.info("Extracting tile geometries from Nexus...")
    geom_query = (f"SELECT distinct(polygon) "
                  f"FROM {input_schema}.raster "
                  f"WHERE dc_id = 48 and "
                  f"parameter LIKE '%%{band_names[0]}%%'")
    
    # store geometries in dataframe and remove query
    geom_df = pd.read_sql_query(geom_query, engine)
    geom_query = None
    
    # terminate function if no tile geometries are available
    if geom_df.empty:
        logging.info("No tile geometries available. Workflow terminated.")
        return
    
    # else, give message that geometries were extracted
    # and continue the workflow
    else:
        logging.info("Tile geometries extracted.")
    
    
    ### CREATING NESTED LOOP TO PERFORM CALCULATIONS DATE- AND TILE-WISE ###
    # initialize loop to do calculations for all dates in time range
    for i in range(len(time_range)):
        
        # create date string
        s2_date = str(time_range[i])
        
        # Give message about (next) iteration
        logging.info("(Next) iteration initiated...")
        
        # set range and initialize loop to perform calculations for each tile
        tile_range = range(len(geom_df)) # run calculations for all tiles
        # tile_range = range(0, 5) # try calculations for small num of tiles
        for j in tile_range:
            
            # extract tile_geometry for current tile from geom_df
            tile_geom = list(geom_df.iloc[j])[0]
            
            
            ### HARVESTING SENTINEL-2 IMAGES ###
            # download selected Sentinel-2 bands
            logging.info("Searching for Sentinel-2 data and "
                         "downloading them if available...")
            tile_id_init, time_stamp_init = download_s2_bands(
            config = config,
            input_schema = input_schema,
            engine = engine,
            s3 = s3,
            band_names = band_names,
            output_path = output_path,
            s2_date = s2_date,
            tile_geom = tile_geom)
            
            # Give message that no Sentinel-2 data were available
            if time_stamp_init == None:
                logging.info("No Sentinel-2 data available for current tile.")
            
            
            ### RESAMPLING AND VEGETATION INDEX CALCULATIONS ###
            else:
                # make copy of tile_id and time_stamp variables
                tile_id, time_stamp = tile_id_init, time_stamp_init
                
                # resample s2 bands to 10m
                logging.info("Sentinel-2 bands downloaded.")
                logging.info("Resampling Sentinel-2 bands...")
                resample_s2_bands(output_path = output_path,
                                  band_names = band_names)
                logging.info("Sentinel-2 bands resampled.")
                
                # calculate and mask vegetation indices
                logging.info("Masking Sentinel-2 bands and calculating "
                             "vegetation indices current tile...")
                calc_veg_indices(output_path = output_path,
                                 s2_date = s2_date,
                                 band_names = band_names,
                                 veg_indices = veg_indices,
                                 tile_index = j)
                logging.info("Vegetation indices current tile calculated.")
                
                # remove individual sentinel-2 bands from data folder 
                s2_files = glob2.glob("./data/surf_refl*")
                s2_files.extend(glob2.glob("./data/s2a_scene_class*"))
                for f in s2_files:
                    os.remove(f)
            
            
            ### MOSAICING AND UPLOADING VEGETATION INDEX IMAGES ###
            veg_ind_tile_files = glob2.glob(f"{output_path}/"
                                            f"{veg_indices[i]}-"
                                            f"{s2_date}-*.tif")
            
            # initiate last iteration of inner loop body
            if j == tile_range[-1] and len(veg_ind_tile_files) != 0:
                
                # mosaicing the vegetation index tiles into one composite
                logging.info("Mosaicing vegetation index tiles "
                             "into one composite...")
                mosaic_veg_indices(output_path = output_path,
                                   s2_date = s2_date,
                                   veg_indices = veg_indices,
                                   time_stamp = time_stamp)
                logging.info("Vegetation index tiles mosaiced "
                             "into one composite.")
                
                # upload mosaiced vegetation index images to Nexus
                logging.info("Uploading vegetation index images to Nexus...")
                upload_veg_indices(config = config,
                                   output_schema = output_schema,
                                   engine = engine,
                                   s3 = s3,
                                   output_path = output_path,
                                   time_stamp = time_stamp,
                                   veg_indices = veg_indices)
                logging.info("Vegetation index images uploaded.")
                
                
                ### PARCEL CALCULATIONS ###
                if calc_parcel_stats == True:
                    
                    # select all grassland parcels within study area
                    # and write to file
                    logging.info("Searching and selecting grassland parcels "
                                 "within study area")
                    parcels, parcels_present = select_parcels(
                    input_schema = input_schema,
                    engine = engine,
                    output_path = output_path,
                    s2_date = s2_date)
                    
                    # give message if no parcels were available
                    if parcels_present == False:
                        logging.info("No grassland parcels available "
                                     "within current tile.")
                    
                    # else, calculate zonal statistics and build the dataframe
                    else:
                        logging.info("Grassland parcels selected.")
                        
                        # calculate zonal statistics
                        logging.info("Calculating zonal statistics...")
                        parcel_stats = calc_zonal_stats(
                        output_path = output_path,
                        veg_indices = veg_indices,
                        time_stamp = time_stamp)
                        logging.info("Zonal statistics calculated.")
                        
                        # create new parcels dataframe and upload to Nexus
                        logging.info("Creating updated parcels dataframe and "
                                     "if not empty, upload it to Nexus...")
                        parcels_new = upload_parcels_df(time_stamp =
                                                        time_stamp,
                                                        tile_id = tile_id,
                                                        cloud_cover_perc =
                                                        cloud_cover_perc,
                                                        parcels = parcels,
                                                        parcel_stats =
                                                        parcel_stats,
                                                        output_table =
                                                        output_table,
                                                        engine = engine,
                                                        output_schema =
                                                        output_schema)
                        
                        # give messages about uploading information to Nexus
                        if not parcels_new.empty:
                            logging.info("Parcel dataframe uploaded.")
                        
                        else:
                            logging.info("Dataframe empty; no new parcel "
                                         "information uploaded to Nexus.")
                        
                        # remove parcel variables
                        parcels, parcel_stats, parcels_new = None, None, None
        
        # empty data folder contents 
        files = os.listdir(output_path)
        for f in files:
            os.remove(f)
        logging.info("Data folder and variables cleared.")
    
    # Give message that workflow was successfully executed
    logging.info("Workflow successfully executed.")


if __name__ == "__main__":
        
    if config.DEBUG:
        # If 'settings.DEBUG' is set to True, Nexus will run provided function
        # with the arguments below. You could specify different test cases 
        # here, for example running the script with different input arguments.
        run_task(input_schema = "knowh2o",
                 output_schema = "sandbox_sen2grass",
                 output_table = "parcels_new",
                 calc_parcel_stats = False)
    
    else:
        # In case 'settings.DEBUG' is False, this script can be run from 
        # the command line. To try it out, open a command prompt, navigate
        # to the current directory and run:
        # <python main.py sandbox sandbox_demo_calculation_python 500 yes True>
        fire.Fire(run_task)
    