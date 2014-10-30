#!/bin/bash
# stdout = 28.679/41.041/53.403/12.362
# being " rtt min/avg/max/mdev = 28.679/41.041/53.403/12.362 ms "

ping -c 3 $1 | tail -1 | awk '{print $4}'
