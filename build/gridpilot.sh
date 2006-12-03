#!/bin/sh

# Max and min vm heap size
export ms="32m"
export mx="128m"

jars=`ls lib/*.jar`
classpath=`echo $jars | sed 's/ /:/g'`

#java -cp $classpath:gridpilot.signed.jar \
#-Djava.security.manager -Djava.security.policy=suresh.policy \
#-Dmessagefile=null -Xms$ms -Xmx$mx gridpilot.GridPilot 

java -cp $classpath:gridpilot.jar -Dmessagefile=null -Xms$ms -Xmx$mx \
gridpilot.GridPilot 

#java -cp .:$classpath -Dmessagefile=null -Xms$ms -Xmx$mx gridpilot.GridPilot 
