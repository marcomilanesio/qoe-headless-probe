#!/bin/bash
re='^[0-9]+$'

conf_file=$1
nr_runs=$2

if [ "$#" -ne 2 ]; then
    echo "Specify a configuration file and the number of runs";
    exit 1;
fi

if ! [[ $2 =~ $re ]] ; then
   echo "Error: Second parameter is not a number" >&2; 
   exit 1;
fi

FLUME_HOME=.toflume

if [ ! -d "$FLUME_HOME" ]; then
	mkdir $FLUME_HOME
fi

BKP_FOLDER_HOME=./session_bkp

if [ ! -d "$BKP_FOLDER_HOME" ]; then
	mkdir $BKP_FOLDER_HOME
fi

NOW=$(date +"%d-%m-%y_%T")
BKP_FOLDER=$BKP_FOLDER_HOME/$NOW
mkdir $BKP_FOLDER

sleep 2
/usr/bin/python probe.py $nr_runs $conf_file $BKP_FOLDER

echo "End of main.sh"

