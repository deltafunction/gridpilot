cd ..
rm -rf gridpilot/*.class
rm -rf gridpilot/*/*/*.class
JARS=`ls lib/*jar | tr  "\n" ":"`
#echo javac -classpath .:$JARS gridpilot/*.java gridpilot/*.java
javac -classpath .:$JARS gridpilot/*.java gridpilot/*/*/*.java
