cp -r ../resources ./
cp -r ../gridpilot ./
cp ../manifest ./
cp ../gridpilot.conf ./
cp ../readme.txt ./
ls resources/certificates > resources/ca_certs_list.txt
sed -e "s/.*date.*/\<\!--date--\>`date`/" resources/about.htm > tmpAbout.htm
mv -f tmpAbout.htm resources/about.htm

#sh compile.sh

rm -f gridpilot.*jar *signed.jar

jarFiles=`ls ../lib/*.jar`
for name in ${jarFiles[@]}
  do
  jar -xvf $name
done

find . -name \*.class > jars
find resources -type f | grep -v CVS >> jars
echo gridpilot.conf >> jars
echo readme.txt >> jars

echo Creating an unsigned applet

first=1800
len=`cat jars | wc -l`
echo Length: $len
((rest = $len - $first))
echo Rest: $rest

jar -cmf manifest gridpilot.jar `head -n $first jars`
jar -uf gridpilot.jar `tail -n $rest jars`

if [ -f suresh.store ]
then
    echo keystore exists
else
    keytool -genkey -keystore suresh.store -alias sureshcert
fi
keytool -storepass "dummy###" -export -keystore suresh.store -alias sureshcert -file suresh.cer
jarsigner -storepass "dummy###" -signedjar gridpilot.signed.jar -keystore suresh.store gridpilot.jar sureshcert -storepass "dummy###"

rm -rf oracle javax com COM cryptix hsqlServlet.class LICENSE.txt log4j.properties META-INF netscape org xjava LDAP*
rm -f ~/.globus/tmp.p12
rm -rf jars gridpilot resources tmp* manifest
