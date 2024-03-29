#!/bin/bash

cp -r ../resources ./
cp -r ../gridpilot ./
cp -r ../lib ./
rm -rf lib/CVS
cp ../gridpilot.conf ./
#ls resources/certificates > resources/ca_certs_list.txt
sed -i -e "s/<\!--date-->/\<\!--date--\>`date`/" resources/about.htm
sed -e "s/<\!--date-->/`date`/" ../README.in > README.txt
unix2dos README.txt

cp resources/aviateur* ./
cp resources/mac_icon.icns ./

# cd ..
# sh compile.sh
# cd build

rm -f gridpilot.*jar *signed.jar

#jarFiles=`ls ../lib/*.jar | grep -v jce-jdk | grep -v bcprov`
#for name in ${jarFiles[@]}
#do
#  jar -xf $name
#done
