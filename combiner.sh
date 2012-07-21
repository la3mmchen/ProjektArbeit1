#!/bin/bash
startTime=`date +%s`
if [ -d $1 ]; then
	echo "Processing: $1"
	for dir in `ls "$1/"`
	do
		echo "-- $1$dir"
		if [ -d "$1$dir" ]; then 
			FILES="$1/$dir/*"
			echo "%% $FILES"
			outFile="./file_packed_$dir.txt"
			`touch $outFile`
			for f in $FILES
			do
				echo "Processing $f to $outFile"
				sed -e :a -e '/$/N; s/\r\n/ /; ta;' $f | tee >> $outFile
			done
		else
			echo "no dir found"
		fi
	done
else
	echo "no dirs found"
fi
timedelta=$((`date +%s` - $startTime))
echo "started: $startTime"
echo "time elapsed: $timedelta"
