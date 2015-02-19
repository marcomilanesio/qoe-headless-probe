#!/bin/bash
re='^[0-9]+$'

default_conf_file=./probe.conf
default_nr_runs=1

conf_file=${1:-$default_conf_file}
nr_runs=${2:-$default_nr_runs}

if [ "$#" -ne 2 ]; then
    echo "Loaded default values: $conf_file [$nr_runs]";
    #exit 1;
else
    if ! [[ -f $1 ]] ; then
        echo "Error: Conf file does not exists" >&2;
        exit 1;
    fi
    if ! [[ $2 =~ $re ]] ; then
        echo "Error: Second parameter is not a number" >&2;
        exit 1;
    fi
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
/usr/bin/python3 probe.py $nr_runs $conf_file $BKP_FOLDER

echo "End of main.sh"

