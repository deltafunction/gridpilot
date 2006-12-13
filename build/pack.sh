#!/bin/bash

version="0.0.2"

mkdir GridPilot-$version

cp -r gridpilot.jar gridpilot-atlas-gen.conf gridpilot.bat gridpilot.sh lib GridPilot-$version/

mv GridPilot-$version/gridpilot-atlas-gen.conf GridPilot-$version/gridpilot.conf

mv GridPilot-$version/gridpilot.jar GridPilot-$version/gridpilot-$version.jar

sed -i 's/gridpilot\.jar/gridpilot-'$version'.jar/' GridPilot-$version/gridpilot.bat

zip -r GridPilot-$version.zip GridPilot-$version

rm -rf GridPilot-$version