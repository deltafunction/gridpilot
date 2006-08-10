cp -r ../resources ./
cp -r ../gridpilot ./
cp ../gridpilot.conf ./
cp ../readme.txt ./
ls resources/certificates > resources/ca_certs_list.txt
sed -e "s/.*date.*/\<\!--date--\>`date`/" resources/about.htm > tmpAbout.htm
mv -f tmpAbout.htm resources/about.htm

# cd ..
# sh compile.sh
# cd build

rm -f gridpilot.*jar *signed.jar

jarFiles=`ls ../lib/*.jar`
for name in ${jarFiles[@]}
  do
  jar -xvf $name
done

find . -type f | grep -v CVS | grep -v META-INF | grep -v suresh | grep -v 'gridpilot\.jar' | grep -v '\.sh' > jars

echo Creating an unsigned applet

first=2000
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
rm -rf jars gridpilot resources tmp* gridpilot.conf readme.txt gridpilot.log
