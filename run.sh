#!/bin/bash
cd $(dirname $0)
source env5101/bin/activate
python shp_compile_v.py  $1
deactivate
