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
import fpformat
import os
import logging
import datetime
import operator

logger = logging.getLogger("Parser")


class Parser():
    def __init__(self, tstatfile, harfile, client_id):
        self.tstatfile = tstatfile
        self.harfile = harfile
        self.client_id = client_id
        self.har_dict = {}

    def parseTstat(self, separator="\n"):
        log = open(self.tstatfile, 'r')
        lines = log.readlines()
        log.close()
        from_har = self.har_dict['entries'].keys()
        found = []
        #json_metrics = ''
        rows = [line[:-1].split(" ") for line in lines]
        for line in rows:
            if line[59] is not "":  # avoid HTTPS sessions
                line[59] = line[59][:-1]
                httpids = line[59].split(",")
                for elem in httpids:
                    if elem in from_har:
                        app_rtt = float(line[15])+float(line[26])
                        metrics = {unicode('local_ip'): line[0], unicode('local_port'): line[1],
                                   unicode('syn_time'): fpformat.fix(line[13], 0),
                                   unicode('app_rtt'): fpformat.fix(app_rtt, 0),
                                   unicode('remote_ip'): line[30],
                                   unicode('remote_port'): line[31],
                                   }
                        self.har_dict['entries'][elem].update(metrics)
                        found.append(elem)

        # if elem is not found in tstat, then find the most similar uri and copy data from that
        remaining = [x for x in from_har if x not in found]
        logger.warning("{0} elements from har file are not logged in tstat".format(len(remaining)))
        matches = {}
        for httpid in remaining:
            uri = self.har_dict['entries'][httpid]['uri']
            length = {}
            for k, v in self.har_dict['entries'].iteritems():
                if k not in remaining:
                    length[k] = len(os.path.commonprefix([uri, v['uri']]))
            candidate = max(length.iteritems(), key=operator.itemgetter(1))[0]
            matches[httpid] = candidate

        for k, v in matches.iteritems():
            ref = self.har_dict['entries'][v]
            metrics = {unicode('local_ip'): ref['local_ip'], unicode('local_port'): ref['local_port'],
                       unicode('syn_time'): ref['syn_time'],
                       unicode('app_rtt'): ref['app_rtt'],
                       unicode('remote_ip'): ref['remote_ip'],
                       unicode('remote_port'): ref['remote_port'],
                       }
            self.har_dict['entries'][k].update(metrics)

        if matches:
            logger.info("{0} objects ingested from similar objects (most similar uri).".format(len(matches)))
        else:
            logger.info("No data ingested.")

    @staticmethod
    def get_datetime(harstr):
        datetimestr = harstr.replace("T", " ")[:harstr.replace("T", " ").rfind("-")]
        return unicode(datetime.datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S.%f'))

    def parseHar(self):
        with open(self.harfile) as hf:
            json_data = json.load(hf)

        data = json_data['log']
        page = data['pages'][0]
        session_url = page['id']
        session_start = Parser.get_datetime(page["startedDateTime"])
        full_load_time = page["pageTimings"]["onLoad"]
        logger.info("Found {0} objects on in the har file.".format(len(data['entries'])))
        self.har_dict = {unicode('session_url'): session_url,
                          unicode('probe_id'): self.client_id,
                          unicode('session_start'): session_start,
                          unicode('full_load_time'): full_load_time,
                          unicode('entries'): None}
        har_metrics = {}
        for entry in data['entries']:
            request_ts = Parser.get_datetime(entry["startedDateTime"])
            firstByte = Parser.get_datetime(entry["TimeToFirstByte"])
            end_ts = Parser.get_datetime(entry["endtimeTS"])

            request = entry['request']
            url = request['url']
            for header in request['headers']:
                if header['name'] == "httpid":
                    httpid = header['value']

            if not httpid:
                logger.error("Unable to find httpid in Har file: skipping {0}".format(url))
                continue

            response = entry['response']
            content = response['content']
            size = content['size']
            mime = content['mimeType'].split(";")[0]  # eliminate charset utf-8 from text

            #timings = entry['timings']
            #wait = timings['wait']
            #receive = timings['receive']
            #time = wait + receive
            time = entry['time']

            #har_metrics = {unicode('session_url'): session_url, unicode('full_load_time'): unicode(full_load_time),
            #               unicode('uri'): url, unicode('request_ts'): request_ts, unicode('content_type'): mime,
            #               unicode('session_start'): session_start, unicode('body_bytes'): unicode(size),
            #               unicode('first_bytes_rcv'): unicode(firstByte), unicode('end_time'): unicode(end_ts),
            #               unicode('rcv_time'): unicode(time), unicode('tab_id'): unicode('0'),
            #               unicode('httpid'): httpid,
            #               unicode('probe_id'): unicode(self.client_id)}
            har_metrics[str(httpid)] = {unicode('uri'): url, unicode('request_ts'): request_ts,
                                        unicode('content_type'): mime, unicode('body_bytes'): unicode(size),
                                        unicode('first_bytes_rcv'): unicode(firstByte),
                                        unicode('end_time'): unicode(end_ts), unicode('rcv_time'): unicode(time)}

            self.har_dict['entries'] = har_metrics

    def parse(self):
        self.parseHar()
        self.parseTstat()
        return self.har_dict

'''
{unicode('session_url'): session_url, unicode('full_load_time'): unicode(onLoad),
unicode('host'): request_host, unicode('uri'): request_url,
unicode('request_ts'): request_ts, unicode('content_type'): cnt_type,
unicode('content_len'): unicode('0'), unicode('session_start'): session_start,
unicode('cache'): unicode('0'), unicode('response_code'): unicode(status),
unicode('get_bytes'): unicode('-1'), unicode('header_bytes'): unicode('-1'),
unicode('body_bytes'): unicode(responde_body_size),
unicode('cache_bytes'): unicode('0'),
unicode('dns_start'): unicode('1970-01-01 01:00:00'),
unicode('dns_time'): unicode(dns),
unicode('syn_start'): unicode('1970-01-01 01:00:00'),
unicode('is_sent'): unicode('0'),
unicode('get_sent_ts'): unicode('1970-01-01 01:00:00'),
unicode('first_bytes_rcv'): unicode(firstByte),
unicode('end_time'): unicode(endTS),
unicode('rcv_time'): unicode(receive), unicode('tab_id'): unicode('0'),
unicode('ping_gateway'): unicode('0'), unicode('ping_google'): unicode('0'),
unicode('annoy'): unicode('0')}
'''
if __name__ == '__main__':
    import os
    harfile = '/tmp/phantomjs.har'
    tstatfile = '/tmp/tstat.out/log_own_complete'
    p = Parser(tstatfile, harfile, 1608989979)
    p.parse()
