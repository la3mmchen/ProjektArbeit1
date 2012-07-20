#!/bin/bash

FILES="/home/alex/Downloads/meta/tags/21/*"
inFile="/home/alex/manyTags.txt"
rm $inFile
for f in $FILES
do
	echo "Processing $f"
#	cat $f >> $inFile
	sed -e :a -e '/$/N; s/\r\n/ /; ta;' $f >> $inFile
done
