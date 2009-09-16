#!/bin/sh

# Max and min vm heap size
export ms="128m"
export mx="1024m"

jars=`ls lib/*.jar`
classpath=`echo $jars | sed 's/ /:/g'`

java -cp `ls gridpilot*.jar`:$classpath -Dmessagefile=null -Xms$ms -Xmx$mx \
gridpilot.GridPilot 
