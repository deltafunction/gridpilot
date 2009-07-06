#!/bin/bash

cp ../GridFactory/build/common.jar ../GridFactory/build/lrms.jar lib/

cd build

./extract.sh
./compile.sh
./makejar.sh

cd ..
