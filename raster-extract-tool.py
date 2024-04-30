import argparse
import s3fs
import os 
import sys
import fiona
import rasterio
import gc
import zarr
import dask
import numpy as np
import xarray as xr
import rioxarray as rxr
import geopandas as gpd
from affine import Affine
from shapely.geometry import Polygon 
from rasterio.mask import mask
from math import ceil
from dotenv import load_dotenv

sys.path.append('/usr/share/qgis/python')
sys.path.append('/usr/share/qgis/python/plugins')
os.environ['QGIS_PREFIX_PATH'] = '/usr'
os.environ['QT_QPA_PLATFORM_PLUGIN_PATH'] = '/usr/lib/x86_64-linux-gnu/qt5/plugins/platforms/'
os.environ['QT_QPA_PLATFORM'] = 'offscreen'

import processing
from qgis.core import * 
from processing.core.Processing import Processing

load_dotenv()

access_key = os.getenv("AWS_ACCESS_KEY") 
secret_key = os.getenv("AWS_SECRET_KEY") 

fs = s3fs.S3FileSystem(key=access_key, secret=secret_key, client_kwargs={'endpoint_url': 'https://us-iad-1.linodeobjects.com'}) 

def tile_id_extractor(res, tile_size, out_path, dest, tile_id, out_crs=None):
    qgs = QgsApplication([], False)
    qgs.initQgis()
    Processing.initialize()

    print('--> Check no. 1.')

    sentinel_tile = rxr.open_rasterio(f'{tile_id}.tif', masked=True)

    if not out_crs:
        out_crs = int(str(sentinel_tile.rio.crs).split(':')[1])

    minx, miny, maxx, maxy = float(sentinel_tile.rio.bounds()[0]), float(sentinel_tile.rio.bounds()[1]), float(sentinel_tile.rio.bounds()[2]), float(sentinel_tile.rio.bounds()[3])

    tile_size = int(tile_size) * 2

    ######### FULL AREA #########

    # extent_str = f"{minx},{maxx},{miny},{maxy} [{sentinel_tile.rio.crs}]"

    # parameters = {
    #                 'EXTENT':extent_str,
    #                 'EXTENT_BUFFER':0,
    #                 'TILE_SIZE':maxx-minx,
    #                 'MAP_UNITS_PER_PIXEL':res,
    #                 'MAKE_BACKGROUND_TRANSPARENT':False,
    #                 'MAP_THEME':None,
    #                 'LAYERS':[f'wms://crs={sentinel_tile.rio.crs}&format&type=xyz&url=https://mt1.google.com/vt/lyrs%3Ds%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D&zmax=22&zmin=0&http-header:referer='],
    #                 'OUTPUT':f'{out_path}/full_extent_raster.tif'
    #         }

    # print('PRE-PROCESSING')
    
    # processing.run('native:rasterize', parameters)

    # print('POST-PROCESSING')

    # raster_ex = rxr.open_rasterio(f'{out_path}/georef_raster_{i}_{j}.tif', masked=True)
    # raster_ex.rio.write_crs(out_crs, inplace=True)
    # raster_ex.rio.to_raster(f'{out_path}/georef_raster_{i}_{j}.tif')

    ######### INDIVIDUAL TILES #########

    x_tiles = ceil((maxx - minx) / (tile_size / 2))
    y_tiles = ceil((maxy - miny) / (tile_size / 2))

    transform = Affine.translation(minx, miny) * Affine.scale(tile_size, -tile_size)

    print('--> Check no. 2.')

    raster_datasets = []

    for i in range(x_tiles):
        for j in range(y_tiles):
            grid_tile = QgsRectangle(minx + i * tile_size / 2, 
                            miny + j * tile_size / 2, 
                            minx + (i + 1) * tile_size / 2, 
                            miny + (j + 1) * tile_size / 2)

            extent_str = f"{grid_tile.xMinimum()},{grid_tile.xMaximum()},{grid_tile.yMinimum()},{grid_tile.yMaximum()} [{sentinel_tile.rio.crs}]"

            parameters = {
                    'EXTENT':extent_str,
                    'EXTENT_BUFFER':0,
                    'TILE_SIZE':tile_size / 2,
                    'MAP_UNITS_PER_PIXEL':res,
                    'MAKE_BACKGROUND_TRANSPARENT':False,
                    'MAP_THEME':None,
                    'LAYERS':[f'wms://crs={sentinel_tile.rio.crs}&format&type=xyz&url=https://mt1.google.com/vt/lyrs%3Ds%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D&zmax=22&zmin=0&http-header:referer='],
                    'OUTPUT':f'{out_path}/georef_raster_{i}_{j}.tif'
            }

            print('--> Check no. 3. In the for loop -- but just BEFORE processing.')

            processing.run('native:rasterize', parameters)

            print('--> Check no. 4. In the for loop -- but just AFTER processing.')

            raster_ex = rxr.open_rasterio(f'{out_path}/georef_raster_{i}_{j}.tif', masked=True, chunks={'x': tile_size, 'y': tile_size})
            raster_ex.rio.write_crs(out_crs, inplace=True)
            raster_ex.rio.to_raster(f'{out_path}/georef_raster_{i}_{j}.tif')
            
            raster_ds = raster_ex.to_dataset(name='data')
            raster_datasets.append(raster_ds)
            
            del raster_ex
            gc.collect()

    combined_ds = xr.combine_by_coords(raster_datasets)
    combined_ds.to_zarr(f'{out_path}/georef_raster.zarr', mode='w', consolidated=True)

    qgs.exitQgis()

    print('--> Check no. 5. Uploading to S3...')

    fs.put(f'{out_path}', f's3://{dest}', recursive=True)

    print('--> Check no. 6. Should be on S3.')

def shapefile_extractor(res, tile_size, out_path, dest, shapefile, out_crs=None):
    qgs = QgsApplication([], False)
    qgs.initQgis()
    Processing.initialize()

    with fiona.open(f'{shapefile}') as shp:
        if not out_crs:
            out_crs = int(str(shp.crs).split(':')[1])

    shape_gpd = gpd.read_file(f'{shapefile}/{shapefile}.shp')
    updated_shape_gpd = shape_gpd.to_crs('epsg:3857')
    geometries = [geom for geom in list(updated_shape_gpd['geometry'])]

    ######### INDIVIDUAL TILES #########

    minx, miny, maxx, maxy = float(updated_shape_gpd.bounds.iloc[0][0]), float(updated_shape_gpd.bounds.iloc[0][2]), float(updated_shape_gpd.bounds.iloc[0][1]), float(updated_shape_gpd.bounds.iloc[0][3])

    tile_size = int(tile_size) * 2

    x_tiles = range(ceil((maxx - minx) / (tile_size / 2)))
    y_tiles = range(ceil((maxy - miny) / (tile_size / 2)))

    transform = Affine.translation(minx, miny) * Affine.scale(tile_size, -tile_size)

    raster_datasets = []

    for i in x_tiles:
        for j in y_tiles:

            grid_tile_poly = Polygon([(minx + i * (tile_size / 2), miny + j * (tile_size / 2)),
                             (minx + (i + 1) * (tile_size / 2), miny + j * (tile_size / 2)),
                             (minx + (i + 1) * (tile_size / 2), miny + (j + 1) * (tile_size / 2)),
                             (minx + i * (tile_size / 2), miny + (j + 1) * (tile_size / 2))])
            
            if not geometries[0].intersects(grid_tile_poly):
                continue
            else: 
                grid_tile = QgsRectangle(minx + i * (tile_size / 2), 
                            miny + j * (tile_size / 2), 
                            minx + (i + 1) * (tile_size / 2), 
                            miny + (j + 1) * (tile_size / 2))

                extent_str = f"{grid_tile.xMinimum()},{grid_tile.xMaximum()},{grid_tile.yMinimum()},{grid_tile.yMaximum()} [{updated_shape_gpd.crs}]"

                parameters = {
                    'EXTENT':extent_str,
                    'EXTENT_BUFFER':0,
                    'TILE_SIZE':tile_size / 2,
                    'MAP_UNITS_PER_PIXEL':res,
                    'MAKE_BACKGROUND_TRANSPARENT':False,
                    'MAP_THEME':None,
                    'LAYERS':[f'wms://crs={updated_shape_gpd.crs}&format&type=xyz&url=https://mt1.google.com/vt/lyrs%3Ds%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D&zmax=22&zmin=0&http-header:referer='],
                    'OUTPUT':f'{out_path}/georef_raster_{i}_{j}.tif'
                }

                print('--> Check no. 1.1. In the for loop -- PRE-processing.')

                processing.run('native:rasterize', parameters)

                print('--> Check no. 1.2. In the for loop -- POST-processing.')

                raster_ex = rxr.open_rasterio(f'{out_path}/georef_raster_{i}_{j}.tif', masked=True, chunks={'x': tile_size, 'y': tile_size})
                raster_ex.rio.write_crs(out_crs, inplace=True)
                raster_ex.rio.to_raster(f'{out_path}/georef_raster_{i}_{j}.tif')

                raster_ds = raster_ex.to_dataset(name='data')
                raster_datasets.append(raster_ds)
                
                del raster_ex
                gc.collect()
        
    combined_ds = xr.combine_by_coords(raster_datasets)
    combined_ds.to_zarr(f'{out_path}/georef_raster.zarr', mode='w', consolidated=True)

    ######### FULL AREA #########

    # extent_str = f"{updated_shape_gpd.bounds.iloc[0][0]},{updated_shape_gpd.bounds.iloc[0][2]},{updated_shape_gpd.bounds.iloc[0][1]},{updated_shape_gpd.bounds.iloc[0][3]} [{updated_shape_gpd.crs}]"            

    # parameters = {
    #             'EXTENT':extent_str,
    #             'EXTENT_BUFFER':0,
    #             'TILE_SIZE':tile_size,
    #             'MAP_UNITS_PER_PIXEL':res,
    #             'MAKE_BACKGROUND_TRANSPARENT':False,
    #             'MAP_THEME':None,
    #             'LAYERS':[f'wms://crs={updated_shape_gpd.crs}&format&type=xyz&url=https://mt1.google.com/vt/lyrs%3Ds%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D&zmax=22&zmin=0&http-header:referer='],
    #             'OUTPUT':f'{out_path}/georef_raster.tif'
    #     }

    # print('--> Check no. 2.1. PRE-processing.')

    # processing.run('native:rasterize', parameters)

    # print('--> Check no. 2.2. POST-processing.')

    # georef_rst = rxr.open_rasterio(f'{out_path}/georef_raster.tif', masked=True)
    # georef_rst.rio.write_crs(out_crs, inplace=True)
    # georef_rst.rio.to_raster(f'{out_path}/georef_raster.tif')

    # with rasterio.open(f'{out_path}/georef_raster.tif', 'r+') as rst:
    #     rst.nodata = None
    #     out_image, out_transform = mask(rst, geometries, crop=True, all_touched=True, invert=False)
    #     out_meta = rst.meta

    #     out_meta.update({
    #     "driver": "GTiff",
    #     "height": out_image.shape[1],
    #     "width": out_image.shape[2],
    #     "transform": out_transform
    # })

    # with rasterio.open(f'{out_path}/georef_raster.tif', 'w', **out_meta) as dest:
    #     dest.write(out_image)

    qgs.exitQgis()

    print('--> Check no. 2.3. PRE--S3 upload.')

    # fs.put(f'{out_path}', f's3://{dest}', recursive=True) # Use below if paramvalidationError encountered.

    fs.put('/root/cyclops/raster-extraction/raster-extraction/dual_shapefile', 's3://cyclops-sbx/tmp/raster-extraction', recursive=True)
    
    print('--> Check no. 2.4. S3 upload complete.')

def main():
    parser = argparse.ArgumentParser(description='Raster extraction from QGIS baselayer map grid.')
    parser.add_argument('--res', required=True, help='Spatial resolution for raster.')
    parser.add_argument('--tile_size', required=True, help='Size of subdivided tiles to be extracted.')
    parser.add_argument('--out_path', required=True, help='Intermediate storage layer.')
    parser.add_argument('--dest', required=True, help='S3 bucket destination.')
    parser.add_argument('--tile_id', required=False, help='Sentinel-2 tile to be extracted.')
    parser.add_argument('--shapefile', required=False, help='Shapefile to perform clipping and extraction against.')
    parser.add_argument('--out_crs', required=False, help='Coordinate reference system for raster.')

    args = parser.parse_args()

    if not (args.shapefile or args.tile_id):
        parser.error('At least one of --shapefile or --tile_id must be provided.')
    
    if args.shapefile and args.tile_id:
        parser.error('Only one of --shapefile or --tile_id should be provided, but not both.')

    if args.tile_id:
        tile_id_extractor(args.res, args.tile_size, args.out_path, args.dest, args.tile_id, args.out_crs)
    else:
        shapefile_extractor(args.res, args.tile_size, args.out_path, args.dest, args.shapefile, args.out_crs)

if __name__ == '__main__':
    main()