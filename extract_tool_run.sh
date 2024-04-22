#!/bin/bash

trap "echo 'Cleaning up...'; export PATH=$OLD_PATH; export PYTHONPATH=$OLD_PYTHONPATH; unset QT_QPA_PLATFORM_PLUGIN_PATH; unset LD_LIBRARY_PATH" EXIT INT

OLD_PATH=$PATH
OLD_PYTHONPATH=$PYTHONPATH

export PATH=/usr/bin:$PATH
export PYTHONPATH=/usr/share/qgis/python:/usr/share/qgis/python/plugins:$PYTHONPATH
export QGIS_PREFIX_PATH=/usr
export QT_QPA_PLATFORM_PLUGIN_PATH=/usr/lib/x86_64-linux-gnu/qt5/plugins/platforms/
export QT_QPA_PLATFORM=offscreen

echo "Using python3 from $(which python3)"

python3 raster-extract-tool.py --minx 507256.8106629999820143 --maxx 519752.8106629999820143 --miny 1732490.7094499999657273 --maxy 1744986.7094499999657273

export PATH=$OLD_PATH
export PYTHONPATH=$OLD_PYTHONPATH
unset QT_QPA_PLATFORM_PLUGIN_PATH
unset LD_LIBRARY_PATH
