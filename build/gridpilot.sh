#!/bin/sh

# Max and min vm heap size
export ms="32m"
export mx="128m"

jars=`ls lib/*.jar`
classpath=`echo $jars | sed 's/ /:/g'`

java -cp `ls gridpilot*.jar`:$classpath -Dmessagefile=null -Xms$ms -Xmx$mx \
gridpilot.GridPilot 
