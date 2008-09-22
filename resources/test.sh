#!/bin/bash

###########################################################################
#
# Test transformation.
# A small shell script to test that GridPilot works.
# It adds the checksums of the md5dums of the input files, multiplies
# with the last input parameter and writes the result to "out.txt".
#
# Used runtime environments: Linux*
#
###########################################################################
if test $# -eq 0; then
  echo "USAGE:   test.sh <multiplier> <input file names>"
  echo "NOTICE: if only <multiplier> is given it defaults to"
  echo "test.sh <multiplier> data1.txt data2.txt"
  echo 
  echo "         <multiplier> ...       job parameter"
  echo "         <input file name> .... name of input files - a comma separated list"

  echo
  echo "EXAMPLE: test.sh 7 data1.txt,data2.txt,data3.txt"
  echo
  exit 0
fi

#########################################################################
# Parameter translation
#

MULT=$1
INFN=$2
if [ "$INFN" = "" ]; then
  INFN="data1.txt,data2.txt"
fi

echo
echo "Input files: $INFN"

OUTFN="out.txt"

export INFN
export MULT
export OUTFN

#########################################################################
# Input files
#

# must be on two lines!
INFNS=`echo $INFN | awk -F, '{OFS=" "; $7=$7; print $0}'`
export INFNS

#########################################################################
# Print information on the system on stdout (goes into log file)
#

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo

startDate=`date +%s`
echo "Start: $startDate"
grep -i MHz /var/log/dmesg
cat /proc/meminfo
uname -a
echo userid="`   id   `"
# this will be registered as metadata
echo HOSTMACHINE: `hostname`
printenv

echo
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
echo

#########################################################################
# The job
#

partsum=0
for infile in $INFNS
do
  add=`md5sum $infile | awk '{print $1}' | cksum | awk '{print $1}'`
  partsum=`expr $partsum + $add`
done

fullsum=0
i=0
until [ "$i" -eq "$MULT" ]
do
i=`expr $i + 1`
fullsum=`expr $fullsum + $partsum`
done

echo $fullsum > $OUTFN

#########################################################################
# Post processing
#

sleep 20

# this will be registered as metadata
echo GRIDPILOT METADATA: OUTPUTFILEBYTES = `du -b $OUTFN | awk '{print $1}'`

endDate=`date +%s`
echo "End date: $endDate"

# this will be registered as metadata
echo GRIDPILOT METADATA: CPUSECONDS = $(($endDate - $startDate))
