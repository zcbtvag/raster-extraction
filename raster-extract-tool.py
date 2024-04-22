import argparse
import s3fs
import os 
import sys
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

def raster_extractor(minx, maxx, miny, maxy, out_path, dest):
    qgs = QgsApplication([], False)
    qgs.initQgis()
    Processing.initialize()

    print('--> Check no. 1.')

    minx, maxx, miny, maxy = float(minx), float(maxx), float(miny), float(maxy)

    ######### FULL AREA #########

    # extent_str = f"{minx},{maxx},{miny},{maxy} [EPSG:3857]"

    # parameters = {
    #                 'EXTENT':extent_str,
    #                 'EXTENT_BUFFER':0,
    #                 'TILE_SIZE':1024, #maxx-minx,
    #                 'MAP_UNITS_PER_PIXEL':0.3,
    #                 'MAKE_BACKGROUND_TRANSPARENT':False,
    #                 'MAP_THEME':None,
    #                 'LAYERS':['wms://crs=EPSG:3857&format&type=xyz&url=https://mt1.google.com/vt/lyrs%3Ds%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D&zmax=22&zmin=0&http-header:referer='],
    #                 'OUTPUT':f'{out_path}/full_extent_raster.tif'
    #         }
    
    # processing.run('native:rasterize', parameters)

    ######### INDIVIDUAL TILES #########

    tile_size = 1000

    x_tiles = range(ceil((maxx - minx) / tile_size))
    y_tiles = range(ceil((maxy - miny) / tile_size))

    print('--> Check no. 2.')

    for i in x_tiles:
        for j in y_tiles:
            grid_tile = QgsRectangle(minx + i * tile_size, 
                            miny + j * tile_size, 
                            minx + (i + 1) * tile_size, 
                            miny + (j + 1) * tile_size)

            extent_str = f"{grid_tile.xMinimum()},{grid_tile.xMaximum()},{grid_tile.yMinimum()},{grid_tile.yMaximum()} [EPSG:3857]"

            parameters = {
                    'EXTENT':extent_str,
                    'EXTENT_BUFFER':0,
                    'TILE_SIZE':tile_size,
                    'MAP_UNITS_PER_PIXEL':0.3,
                    'MAKE_BACKGROUND_TRANSPARENT':False,
                    'MAP_THEME':None,
                    'LAYERS':['wms://crs=EPSG:3857&format&type=xyz&url=https://mt1.google.com/vt/lyrs%3Ds%26x%3D%7Bx%7D%26y%3D%7By%7D%26z%3D%7Bz%7D&zmax=22&zmin=0&http-header:referer='],
                    'OUTPUT':f'{out_path}/raster_{i}_{j}.tif'
            }

            print('--> Check no. 3. In the for loop -- but just BEFORE processing.')

            processing.run('native:rasterize', parameters)

            print('--> Check no. 4. In the for loop -- but just AFTER processing.')

    qgs.exitQgis()

    print('--> Check no. 5. Uploading to S3...')

    fs.put(f'{out_path}', f's3://{dest}', recursive=True)

    print('--> Check no. 6. Should be on S3.')

def main():
    parser = argparse.ArgumentParser(description='Raster extraction from QGIS baselayer map grid.')
    parser.add_argument('--minx', required=True, help='Min x value from bounding box.')
    parser.add_argument('--maxx', required=True, help='Max x value from bounding box.')
    parser.add_argument('--miny', required=True, help='Min y value from bounding box.')
    parser.add_argument('--maxy', required=True, help='Max y value from bounding box.')
    parser.add_argument('--out_path', required=True, help='Intermediate storage layer.')
    parser.add_argument('--dest', required=True, help='S3 bucket destination.')

    args = parser.parse_args()

    raster_extractor(args.minx, args.maxx, args.miny, args.maxy, args.out_path, args.dest)

if __name__ == '__main__':
    main()