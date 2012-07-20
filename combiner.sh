#!/bin/bash

FILES=$1
inFile=$2
if [ -d $1 ]; then
	FILES=$1"*"
	rm $inFile
	for f in $FILES
	do
		echo "Processing $f"
		sed -e :a -e '/$/N; s/\r\n/ /; ta;' $f >> $inFile
	done
else
	echo "no dir found"
fi
