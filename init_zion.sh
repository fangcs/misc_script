#!/bin/bash

nodename=("zion-1" "zion-2" "zion-3" "zion-4" "zion-5" "zion-6" "zion-7" "zion-8" "zion-9" "zion-10" "zion-11" "zion-12")

for thisnode in "${nodename[@]}"
do
	echo "processing node: $thisnode"
#ssh $thisnode 'mkdir -p /scratch/lfang/hadoop26'
	ssh $thisnode 'mkdir -p /scratch/lfang/hadoop_gc_logs'
done
