

#Crawler
javac CrawlMain.java

###################
#java indexer <input directory> <outputdirectory> <query string>
#<input directory> - location where crawler stored documents
#<output directory> - location where Indexer should store indexed files of FSDirectory()
#query string - query string
###################

java CrawlMain $1 $2 $3 $4


#!/bin/sh
#
#echo Crawler script
#
#arg1=$1
#arg2=$2
#arg3=$3
#arg4=$4
#
#echo $arg1 $arg2 $arg3 $arg4
#
#jar_name=Search-Engine.jar
#
##javac Sample.java arg1 arg2
#echo java -jar $jar_name $arg1 $arg2 $arg3 $arg4