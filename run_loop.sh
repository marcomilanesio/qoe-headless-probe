#!/bin/bash
NUM=0
while true; do 
./main.sh probe.conf 2 || true
NUM=$[$NUM + 1]
echo "executed run $NUM"
echo "sleeping 3 secs..."
sleep 3
echo "restarting"
done

