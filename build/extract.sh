#!/bin/bash

cp -r ../resources ./
cp -r ../gridpilot ./
cp -r ../lib ./
rm -rf lib/CVS
cp ../gridpilot.conf ./
ls resources/certificates > resources/ca_certs_list.txt
sed -e "s/.*date.*/\<\!--date--\>`date`/" resources/about.htm > tmpAbout.htm
mv -f tmpAbout.htm resources/about.htm

# cd ..
# sh compile.sh
# cd build

rm -f gridpilot.*jar *signed.jar

#jarFiles=`ls ../lib/*.jar | grep -v jce-jdk | grep -v bcprov`
#for name in ${jarFiles[@]}
#do
#  jar -xf $name
#done