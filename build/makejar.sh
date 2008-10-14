#!/bin/bash

find . -type f | grep -v .jar | grep -v bcprov | grep -v jarfiles | grep -v tmp | grep -v CVS | \
grep -v META-INF | grep -v '\.java' | grep -v '\.zip' | grep -v '\.conf'  | grep -v '\.bat' | \
grep -v suresh | grep -v 'gridpilot\.jar' | grep -r -v '/\.' | grep -r -v '^./[^/]*\.sh' > jarfiles

echo Creating an unsigned applet

length=2000
totalFiles=`cat jarfiles | wc -l`
((steps = $totalFiles  / $length))
((rest = $totalFiles - $steps * $length))
echo Total number of files: $totalFiles
echo Steps: $steps
echo Rest: $rest

echo Packing: 1 : $length
jar -cmf manifest gridpilot.jar `head -n $length jarfiles`

((realSteps = $steps - 1))
for i in `seq 1 $realSteps`
do
((start = $i * $length + 1))
((rem = $totalFiles - $start + 1))
echo Packing: start: $start : length $length from remaining $rem
jar -uf gridpilot.jar `tail -n $rem jarfiles | head -n $length`
done 

if [ $rest -gt 0 ]
then
  ((fin = $steps * $length))
  echo Packing: $fin : $rest
  jar -uf gridpilot.jar `tail -n $rest jarfiles`
fi

#rm -rf oracle javax com COM cryptix hsqlServlet.class LICENSE.txt log4j.properties META-INF netscape org xjava LDAP* *.properties axis* Jacksum* electric* diskCache* help jonelo LGPL* LICENSE* README.txt samples soaprmi sxt tests xpp
#rm -f ~/.globus/tmp.p12
rm -rf jarfiles gridpilot resources tmp* readme.txt

exit

if [ -f suresh.store ]
then
    echo keystore exists
else
    keytool -genkey -keystore suresh.store -alias sureshcert
fi
keytool -storepass "dummy###" -export -keystore suresh.store -alias sureshcert -file suresh.cer
echo signing gridpilot.jar
jarsigner -storepass "dummy###" -signedjar gridpilot.signed.jar -keystore suresh.store gridpilot.jar sureshcert -storepass "dummy###"

jarFiles=`ls lib/*.jar | grep -v jce-jdk | grep -v bcprov | grep -v activation | grep -v mailapi`
for name in ${jarFiles[@]}
do
  echo signing $name
  jarsigner -storepass "dummy###" -keystore suresh.store \
  $name sureshcert -storepass "dummy###"
done

