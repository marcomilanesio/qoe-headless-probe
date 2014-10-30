#!/usr/bin/python
#
# mPlane QoE Probe
#
# (c) 2013-2014 mPlane Consortium (http://www.ict-mplane.eu)
#               Author: Salvatore Balzano <balzano@eurecom.fr>
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see <http://www.gnu.org/licenses/>.
#
import json
from pprint import pprint
import os
import glob
import sys
import os
import re
import datetime
import time
import re
import fpformat




def parseTstat(filename,separator, client_id):
    # Read tstat log
    log = open(filename, "r")
    lines = log.readlines()
    log.close()
    rows = []
    for line in lines:
        line = line[:-1]
        rows.append(line.split(" "))
    # Create json file from tstat metrics
    jsonmetrics = ""
    for line in rows:
        if line[59] is not "":		#avoid HTTPS sessions
            line[59] = line[59][:-1]
            httpids = line[59].split(",")
            #print httpids
            for elem in httpids:
                app_rtt = float(line[15])+float(line[26])
                metrics = {'local_ip': line[0], 'local_port': line[1], 'probe_id': unicode(client_id), 'syn_time': fpformat.fix(line[13],0),
                           'app_rtt': fpformat.fix(app_rtt,0), 'remote_ip': line[30], 'remote_port': line [31],'httpid': elem}
                jsonmetrics = jsonmetrics + json.dumps(metrics) + "\n"
    return jsonmetrics.split(separator)


def updatebyHar(tstatdata,filename):
    try:
        json_data=open(filename)
        data = json.load(json_data)

        # Global metrics of the session
        version = data["log"]["creator"]["version"]
        session_url = data["log"]["entries"][0]["request"]["url"]	# session_url is the url of the first request
        session_start = data["log"]["pages"][0]["startedDateTime"].replace('T', ' ')[0:-1]
        onContentLoad = data["log"]["pages"][0]["pageTimings"]["onContentLoad"]
        onLoad = data["log"]["pages"][0]["pageTimings"]["onLoad"]

        # Parsing each object
        for entry in data["log"]["entries"]:            
            request_ts=entry["startedDateTime"].replace('T', ' ')[0:-1] #human readable time
            firstByte = entry["TimeToFirstByte"].replace('T', ' ')[0:-1]
            endTS = entry["endtimeTS"].replace('T', ' ')[0:-1]

            for field in entry["request"]["headers"]:
            	if field["name"] == "httpid":
                	http_id = field["value"]
            	else:
                	http_id = "null"
            request_host = entry["request"]["url"].split('/')[2]
            request_host=request_host.split(':')[0] # e.g. 'gzip.static.woot.com:9090'
            method = entry["request"]["method"]
            httpVersion = entry["request"]["httpVersion"]
            status = entry["response"]["status"]
            request_url = entry["request"]["url"]
            response_header_size = entry["response"]["headersSize"]
            responde_body_size = entry["response"]["bodySize"]
            cnt_type = entry["response"]["content"]["mimeType"].split(';')[0]  # e.g. 'text/javascript; charset=UTF-8'
            # Timing
            blocked = entry["timings"]["blocked"]
            dns = entry["timings"]["dns"]
            connect = entry["timings"]["connect"]
            send = entry["timings"]["send"]
            wait = entry["timings"]["wait"]	#Not used
            receive = entry["timings"]["receive"]

            # Matching tstatdata
            for line in tstatdata:
                if line["httpid"] == http_id:
                    #fields_to_add = {'log'.decode('utf-8'): "null".decode('utf-8')}
                    fields_to_add = {unicode('session_url'): session_url,unicode('full_load_time'): unicode(onLoad),
                    unicode('host'): request_host, unicode('uri'): request_url,
                    unicode('request_ts'): request_ts, unicode('content_type'): cnt_type, unicode('content_len'): unicode('0'),
                    unicode('session_start'): session_start,
                    unicode('cache'): unicode('0'), unicode('response_code'): unicode(status), unicode('get_bytes'): unicode('-1'),
                    unicode('header_bytes'): unicode('-1'), unicode('body_bytes'): unicode(responde_body_size),
                    unicode('cache_bytes'): unicode('0'), unicode('dns_start'): unicode('1970-01-01 01:00:00'),
                    unicode('dns_time'): unicode(dns), unicode('syn_start'): unicode('1970-01-01 01:00:00'),
                    unicode('is_sent'): unicode('0'), unicode('get_sent_ts'): unicode('1970-01-01 01:00:00'),
                    unicode('first_bytes_rcv'): unicode(firstByte),
                    unicode('end_time'): unicode(endTS), unicode('rcv_time'): unicode(receive), unicode('tab_id'): unicode('0'),
                    unicode('ping_gateway'): unicode('0'), unicode('ping_google'): unicode('0'), unicode('annoy'): unicode('0')}

                    line.update(fields_to_add)
                #print line
            #  End for cicle (matching tstatdata)

        json_data.close()
    except:
        pass

    return tstatdata


