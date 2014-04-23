#!/bin/bash

PSCONTENT=`ps aux | grep "edu\.cmu\.graphchi\.apps" | grep java`

if [ -z "$PSCONTENT" ]
then
	echo "Cannot find any PROCESS whose cmd contains edu.cmu.graphchi.apps and java"
	exit 1
fi

PID=`echo $PSCONTENT | sed "s/\s+/ /g" | cut -d ' ' -f2`
echo PID is $PID
DURATION=1
DELIM="\\t"

#if [ $# != 1 ]
#then 
#	echo "Usage: mem_usage.sh PID"
#	exit 1
#fi

title="data"$DELIM"total"$DELIM"rss"$DELIM"dirty"
echo -e $title

while [ 1 = 1 ]
do
	temp=`pmap -x $PID | grep total | grep -oP "[0-9]+.*"| sed "s/\s\+/ /g"`
	total=`echo $temp | cut -d ' ' -f1`
	rss=`echo $temp | cut -d ' ' -f2`
	dirty=`echo $temp | cut -d ' ' -f3`
	date=`date +20%y-%m-%d-%H-%M-%S`
	echo -e "$date$DELIM$total$DELIM$rss$DELIM$dirty"
	sleep $DURATION
done
