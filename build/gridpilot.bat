@echo off
setlocal ENABLEDELAYEDEXPANSION
if defined CLASSPATH (set CLASSPATH=%CLASSPATH%;.) else (set CLASSPATH=.)
FOR /R .\lib %%G IN (*.jar) DO set CLASSPATH=!CLASSPATH!;%%G
set CLASSPATH=gridpilot.jar;%CLASSPATH%

java -Dmessagefile=null -Xms128m -Xmx1024m gridpilot.GridPilot
