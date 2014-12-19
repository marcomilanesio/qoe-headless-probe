#!/bin/bash
rm -rf /tmp/tstat.out
rm -rf /tmp/*.har
rm probe.log
rm -rf session_bkp/
/home/marco/coding_tmp/setup/NEW/stop.out
psql mplane -c 'drop table probe_id, plugin_raw, active, aggregate_summary, aggregate_details, local_diag;'
rm *.traceroute
