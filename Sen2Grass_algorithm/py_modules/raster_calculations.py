from osgeo import gdal
import glob2
import numpy as np
import os


def calc_veg_indices(output_path, s2_date, band_names,
                     veg_indices, tile_index):
    """
    This function masks the downloaded sentinel-2 bands,
    calculates a number of vegetation images (NDVI, WDVI, NDRE, CI_Red_Edge)
    using the s2 bands as input, and stores them in the data output folder.
    Function argument(s):
    - output_path: the directory where the sentinel-2 bands are stored
    - s2_date = the date for which calculations should be performed
    - band_names: the names of the bands in a list ("scene_class",
                                                    "surf_refl_665nm",
                                                    "surf_refl_705nm",
                                                    "surf_refl_783nm",
                                                    "surf_refl_865nm")
    - veg_indices: the names of the vegetation indices
    - tile_index: the index number of the current tile in the iteration
    """
    # set how numpy floating-point errors should be handled
    np.seterr(divide = "ignore", invalid = "ignore")
    
    # create empty dictionary to store sentinel-2 bands and
    # initiate loop to iterate through bands
    s2_dict = {}
    for i in range(len(band_names)):
        
        # set file path and read raster file as array with float values
        file_path = glob2.glob(os.path.join(output_path,
                                            f"*{band_names[i]}"
                                            f"*{s2_date}*10m.tif"))[0]
        gdal_file = gdal.Open(file_path)
        gdal_band = gdal_file.GetRasterBand(1)
        s2_dict[f"{band_names[i]}"] = (gdal_band.ReadAsArray().
                                       astype(np.float32))
        
        # calculations for SCL band
        if band_names[i] == "scene_class":
            
            # set up coordinate reference system for output GeoTIFF
            geo_trans = gdal_file.GetGeoTransform()
            proj_info = gdal_file.GetProjection()
            
            # convert SCL band categories such as cloud cover to NA
            s2_dict["scene_class"][np.logical_or(s2_dict["scene_class"] < 1,
            np.logical_and(s2_dict["scene_class"] > 7,
                           s2_dict["scene_class"] < 10))] = np.nan
            
            # convert SCL band categories such as land and vegetation to 1
            s2_dict["scene_class"][np.logical_or(s2_dict["scene_class"] >= 10,
            np.logical_and(s2_dict["scene_class"] >= 1,
                           s2_dict["scene_class"] <= 7))] = 1
        
        # calculations for optical bands
        else:
            
            # divide pixel values of optical bands in by quantification value
            # of 10000 to convert digital numbers into reflectance values
            s2_dict[f"{band_names[i]}"] = s2_dict[f"{band_names[i]}"] / 10000
            
            # mask the optical bands with SCL band
            s2_dict[f"{band_names[i]}"] = (s2_dict[f"{band_names[i]}"] *
                                           s2_dict["scene_class"])
        
        # remove gdal variables
        gdal.Unlink(file_path)
        gdal_file, gdal_band = None, None
    
    # create empty dictionary to store vegetation index rasters
    veg_ind_dict = {}
    
    # calculate ndvi
    veg_ind_dict[f"{veg_indices[0]}"] = ((s2_dict["surf_refl_865nm"] -
                                          s2_dict["surf_refl_665nm"]) /
                                         (s2_dict["surf_refl_865nm"] +
                                          s2_dict["surf_refl_665nm"]))
    
    # calculate wdvi
    veg_ind_dict[f"{veg_indices[1]}"] = (s2_dict["surf_refl_865nm"] -
                                         1.8 * s2_dict["surf_refl_665nm"])
    
    # calculate ndre
    veg_ind_dict[f"{veg_indices[2]}"] = ((s2_dict["surf_refl_865nm"] -
                                          s2_dict["surf_refl_705nm"]) /
                                         (s2_dict["surf_refl_865nm"] +
                                          s2_dict["surf_refl_705nm"]))
    
    # calculate ci_red_edge
    veg_ind_dict[f"{veg_indices[3]}"] = (s2_dict["surf_refl_783nm"] /
                                         s2_dict["surf_refl_705nm"] - 1)
    
    # remove Sentinel-2 bands dictionary
    s2_dict = None
    
    # initiate loop to store each vegetation image as file
    for i in range(len(veg_ind_dict)):
        
        # get vegetation index raster from dictionary
        veg_ind_ras = list(veg_ind_dict.values())[i]
        
        # remove outliers and set no data value to -9999
        veg_ind_ras[np.logical_or(veg_ind_ras < -10,
                                  veg_ind_ras > 10)] = -9999
        veg_ind_ras[np.isnan(veg_ind_ras)] = -9999
        
        # create vegetation index output file name
        veg_ind_out_file = (f"{output_path}/{veg_indices[i]}-"
                            f"{s2_date}-{tile_index}.tif")
        
        # perform processing if vegetation image does not exist
        if not os.path.isfile(veg_ind_out_file):
            
            # set number of pixels in x and y
            x_pixels = veg_ind_ras.shape[1]
            y_pixels = veg_ind_ras.shape[0]
            
            # create driver using driver name, output file name,
            # x and y pixels, number of bands and datatype
            driver = gdal.GetDriverByName("GTiff")
            index_data = driver.Create(utf8_path = veg_ind_out_file,
                                       xsize = x_pixels,
                                       ysize = y_pixels,
                                       bands = 1,
                                       eType = gdal.GDT_Float32,
                                       options = ["COMPRESS=LZW"])
            
            # Set vegetation index array as output raster band
            index_data.GetRasterBand(1).WriteArray(veg_ind_ras)
            
            # set GeoTransform parameters and projection on
            # the output file, close data file and remove variables
            index_data.SetGeoTransform(geo_trans) 
            index_data.SetProjection(proj_info)
            index_data.FlushCache()
            index_data = None


def mosaic_veg_indices(output_path, s2_date, veg_indices, time_stamp):
    """
    Function to merge a number of input rasters into one composite image.
    Function argument(s):
    - output_path: the directory where the sentinel-2 bands are stored
    - s2_date = the date for which calculations should be performed
    - veg_indices: the names of the vegetation indices in a list
    - time_stamp: date and time for which calculations were performed
      (in <YYYY><MM><DD> format)
    """
    # initiate loop to iterate over the four vegetation indices
    for i in range(len(veg_indices)):
                    
        # create path name to search for vegetation index images
        veg_ind_in_files = glob2.glob(f"{output_path}/{veg_indices[i]}-"
                                      f"{s2_date}-*.tif")
        
        # create path names for geotiff and vrt output files
        veg_ind_vrt = (f"{output_path}/{time_stamp}-{veg_indices[i]}.vrt")
        veg_ind_out_tiff = veg_ind_vrt.replace(".vrt", ".tif")
        
        # create virtual mosaic from input rasters
        gdal.BuildVRT(destName = veg_ind_vrt,
                      srcDSOrSrcDSTab = veg_ind_in_files,
                      srcNodata = -9999,
                      VRTNodata = -9999)
        
        # store the mosaic as GeoTIFF in output folder
        gdal.Translate(destName = veg_ind_out_tiff,
                       srcDS = veg_ind_vrt,
                       format = "GTiff",
                       creationOptions = ["COMPRESS=LZW"])


def upload_veg_indices(config, output_schema, engine, s3,
                       output_path, time_stamp, veg_indices):
    """
    Function to uplaad calculated vegetation indices to Nexus.
    Function argument(s):
    - config: a configuration file
    - output_schema: the output schema from the Nexus database
      where the tables are stored
    - engine: an engine to connect to a sql database
    - s3: variable for connection with boto3 client
    - output_path: the image path where the sentinel-2 bands are stored
    - time_stamp: date and time for which calculations were performed
      (in <YYYY><MM><DD> format)
    - veg_indices: a list with vegetation indices (NDVI, WDVI,
                                                   NDRE, CI_Red_Edge)
    """
    for i in range(len(veg_indices)):
        file_path = glob2.glob(f"{output_path}/{time_stamp}-"
                               f"{veg_indices[i]}*.tif")[0]
        file_name = os.path.basename(file_path)
        if os.path.isfile(file_path):
            with open(file_path, "rb") as f:
                s3.upload_fileobj(f, config.BUCKET,
                                  f"{output_schema}/raster/{file_name}")