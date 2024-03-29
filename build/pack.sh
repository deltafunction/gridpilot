#!/bin/bash

version="0.2.0"

mkdir GridPilot-$version

cp -r gridpilot.jar gridpilot.conf gridpilot.bat gridpilot.sh lib GridPilot-$version/

mv GridPilot-$version/gridpilot.jar GridPilot-$version/gridpilot-$version.jar

jars=`ls lib | awk '{print";lib/"$1}'`

jars=`echo $jars | sed 's/ ;/;/g'`

sed -i 's|gridpilot\.jar|gridpilot-'$version'.jar'$jars'|' GridPilot-$version/gridpilot.bat

zip -r GridPilot-$version.zip GridPilot-$version

rm -rf GridPilot-$version
