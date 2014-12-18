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


logger = logging.getLogger("Parser")

class Parser():
    def __init__(self, tstatfile, harfile, client_id):
        self.tstatfile = tstatfile
        self.harfile = harfile
        self.client_id = client_id
        #self.tstat_to_process = []
        #self.har_to_process = []
        self.merged = {}
        self.parsed = {}
        self.har_dict = {}

    def parseTstat(self, separator="\n"):
        log = open(self.tstatfile, 'r')
        lines = log.readlines()
        log.close()
        #json_metrics = ''
        rows = [line[:-1].split(" ") for line in lines]
        for line in rows:
            if line[59] is not "":  # avoid HTTPS sessions
                line[59] = line[59][:-1]
                httpids = line[59].split(",")
                for elem in httpids:
                    app_rtt = float(line[15])+float(line[26])
                    metrics = {'local_ip': line[0], 'local_port': line[1], 'probe_id': unicode(self.client_id),
                               'syn_time': fpformat.fix(line[13], 0),
                               'app_rtt': fpformat.fix(app_rtt, 0),
                               'remote_ip': line[30],
                               'remote_port': line[31],
                               'httpid': elem}
                    #self.tstat_to_process.append(metrics)
                    if elem not in self.parsed.keys():
                        self.parsed[elem] = metrics
                    else:
                        logger.warning("parseTstat: Found duplicate: {0}".format(elem))
                        #print metrics
                        #print self.parsed[elem]
                #json_metrics = json_metrics + json.dumps(metrics) + "\n"
        #print self.parsed

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

        '''
        # Global metrics of the session
        version = data["log"]["creator"]["version"]
        session_url = data["log"]["entries"][0]["request"]["url"]  # session_url is the url of the first request
        session_start = data["log"]["pages"][0]["startedDateTime"].replace('T', ' ')[0:-1]
        onContentLoad = data["log"]["pages"][0]["pageTimings"]["onContentLoad"]
        onLoad = data["log"]["pages"][0]["pageTimings"]["onLoad"]

        for entry in data["log"]["entries"]:
            request_ts = entry["startedDateTime"].replace('T', ' ')[0:-1]  # human readable time
            firstByte = entry["TimeToFirstByte"].replace('T', ' ')[0:-1]
            endTS = entry["endtimeTS"].replace('T', ' ')[0:-1]

            for field in entry["request"]["headers"]:
                if field["name"] == "httpid":
                    http_id = field["value"]
                else:
                    http_id = "null"

            request_host = entry["request"]["url"].split('/')[2]
            request_host = request_host.split(':')[0]  # e.g. 'gzip.static.woot.com:9090'
            method = entry["request"]["method"]
            httpVersion = entry["request"]["httpVersion"]
            status = entry["response"]["status"]
            request_url = entry["request"]["url"]
            response_header_size = entry["response"]["headersSize"]
            responde_body_size = entry["response"]["bodySize"]
            try:
                cnt_type = entry["response"]["content"]["mimeType"].split(';')[0]  # e.g. 'text/javascript; charset=UTF-8'
            except:
                logger.error("cnt_type: try to split ( {0} )".format(entry["response"]["content"]["mimeType"]))
                cnt_type = ''
            # Timing
            blocked = entry["timings"]["blocked"]
            dns = entry["timings"]["dns"]
            connect = entry["timings"]["connect"]
            send = entry["timings"]["send"]
            wait = entry["timings"]["wait"]	 # Not used
            receive = entry["timings"]["receive"]

            har_metrics = {unicode('session_url'): session_url, unicode('full_load_time'): unicode(onLoad),
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
                           unicode('annoy'): unicode('0'),
                           u'httpid': http_id,
                           'probe_id': unicode(self.client_id)}

            self.har_dict[str(http_id)] = har_metrics
            #self.har_to_process.append(har_metrics)
            '''

    def har_from_tstat(self):
        logger.debug("Enrich hardic {0} with data from tstatdic {1}".format(len(self.har_dict), len(self.parsed)))
        for x in self.har_dict.keys():
            if x in self.parsed.keys():
                self.har_dict[x].update(self.parsed[x])
                self.merged[x] = self.har_dict[x]
                del self.har_dict[x]
                del self.parsed[x]
        logger.debug("Enriched {0} entries into merged: remaining {1} from tstat, {2} from har".format(len(self.merged), len(self.parsed), len(self.har_dict)))

    def merge_remaining_tstat(self):
        new = {}
        logger.debug("Merging remaining {0} tstat data into merged".format(len(self.parsed)))
        for httpid, dic in self.parsed.iteritems():
            remote = dic['remote_ip']
            #print remote
            for httpid_merged, dic_merged in self.merged.iteritems():
                remote_merged = dic_merged['remote_ip']
                if remote == remote_merged:
                    tmp = dic_merged
                    tmp.update(dic)
                    new[httpid] = tmp
                    #print "found:", httpid, httpid_merged
        self.merged.update(new)
        logger.debug("Enriched {0} data into merged".format(len(self.merged)))

    def merge_remaining_har(self):
        logger.debug("Merging remaining {0} har data into merged".format(len(self.har_dict)))
        new = {}
        for httpid, dic in self.har_dict.iteritems():
            uri = dic['uri']
            max_len = 0
            candidate = {}
            for httpid_merged, dic_merged in self.merged.iteritems():
                uri_merged = dic_merged['uri']
                #print uri_merged
                prefix = os.path.commonprefix([uri, uri_merged])
                c_len = prefix.count("/")
                if c_len >= 3 and c_len > max_len:
                    candidate = dic_merged
                    max_len = c_len
            candidate.update(dic)
            new[httpid] = candidate
        self.merged.update(new)

    def merge(self):
        self.har_from_tstat()
        logger.debug("{0} from tstat, {1} from har not merged (not present in both)".format(len(self.parsed), len(self.har_dict)))
        self.merge_remaining_tstat()
        #print "{0} remaining from tstat".format(len(tstatdic))
        self.merge_remaining_har()
        #print "{0} remaining from har".format(len(hardic))
        #pprint (merged)
        logger.info("{0} totalsize. {1} new items (found candidates)".format(len(self.merged), len(self.har_dict) - (len(self.har_dict) - len(self.parsed))))

    def parse(self):
        self.parseHar()
        from pprint import pprint
        pprint(self.har_dict)
        exit()
        self.parseTstat()
        self.merge()
        error = []
        for k, v in self.merged.iteritems():
            if 'remote_ip' not in v.keys() or v['remote_ip'] == '':
                logger.error("remote_ip not found {0}: {1}".format(k, v.keys()))
                error.append(k)
                continue
        return self.merged, error

'''
    def merge_to_process(self):
        #print len(self.tstat_to_process)
        #print len(self.har_to_process)
        #print len(self.merged)
        for elem in self.har_to_process:
            for t_elem in self.tstat_to_process:
                if elem['httpid'] == t_elem['httpid']:
                    elem.update(t_elem)
                    self.merged.append(elem)

        logger.debug("Merged {0} elements so far [t:{1}, h:{2}]...".format(
            len(self.merged), len(self.tstat_to_process), len(self.har_to_process)))

        merged_httpids = [x['httpid'] for x in self.merged]
        for elem in self.har_to_process:
            if elem['httpid'] not in merged_httpids:
                fake = self._build_fake(elem)
                self.merged.append(fake)

        logger.debug("Got {0} elems from tstat / {1} from har file: returning {2} elems.".format(
            len(self.tstat_to_process), len(self.har_to_process), len(self.merged)))


    def _build_fake(self, el):
        logger.warning("Building fake elem: TSTAT did not get all streams.")
        uri = el['uri']
        cur_candidate = self.merged[0]
        max_len = len(os.path.commonprefix([uri, cur_candidate['uri']]))
        for cur in self.merged:
            if len(os.path.commonprefix([uri, cur['uri']])) > max_len:
                max_len = len(os.path.commonprefix([uri, cur['uri']]))
                cur_candidate = cur
        tmp = cur_candidate
        tmp.update(el)
        return tmp

    def parse(self):
        self.parseTstat()
        self.parseHar()
        self.merge()
        #self.merge_to_process()
        #return self.merged


    def merge_to_process(self):
        cnt = 0
        for t in self.to_process:
            pass


        for h in self.har_to_process:
            if h['httpid'] not in tmp:
                cnt += 1
                cur = self.merged[0]
                max_len = len(os.path.commonprefix([h['uri'], cur['uri']]))
                for t in self.merged:
                    if max_len > len(os.path.commonprefix([h['uri'], t['uri']])):
                        continue
                    else:
                        max_len = len(os.path.commonprefix([h['uri'], t['uri']]))
                        cur = t

                fake = h
                for c in cur.keys():
                    if c not in fake.keys():
                        fake[c] = cur[c]

                self.merged.append(fake)

def parseTstat(filename, separator, client_id):
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
        if line[59] is not "":		# avoid HTTPS sessions
            line[59] = line[59][:-1]
            httpids = line[59].split(",")
            #print httpids
            for elem in httpids:
                app_rtt = float(line[15])+float(line[26])
                metrics = {'local_ip': line[0], 'local_port': line[1], 'probe_id': unicode(client_id),
                           'syn_time': fpformat.fix(line[13], 0),
                           'app_rtt': fpformat.fix(app_rtt, 0),
                           'remote_ip': line[30],
                           'remote_port': line[31],
                           'httpid': elem}
                jsonmetrics = jsonmetrics + json.dumps(metrics) + "\n"
    return jsonmetrics.split(separator)

def updatebyHar(tstatdata, filename):
    httpid_from_tstat = [line['httpid'] for line in tstatdata]
    httpid_from_har = []
    try:
        json_data = open(filename)
        data = json.load(json_data)

        # Global metrics of the session
        version = data["log"]["creator"]["version"]
        session_url = data["log"]["entries"][0]["request"]["url"]  # session_url is the url of the first request
        session_start = data["log"]["pages"][0]["startedDateTime"].replace('T', ' ')[0:-1]
        onContentLoad = data["log"]["pages"][0]["pageTimings"]["onContentLoad"]
        onLoad = data["log"]["pages"][0]["pageTimings"]["onLoad"]

        # Parsing each object
        for entry in data["log"]["entries"]:            
            request_ts = entry["startedDateTime"].replace('T', ' ')[0:-1]  # human readable time
            firstByte = entry["TimeToFirstByte"].replace('T', ' ')[0:-1]
            endTS = entry["endtimeTS"].replace('T', ' ')[0:-1]

            for field in entry["request"]["headers"]:
                if field["name"] == "httpid":
                    http_id = field["value"]
                else:
                    http_id = "null"

            if http_id not in httpid_from_tstat:
                httpid_from_har.append(http_id)

            request_host = entry["request"]["url"].split('/')[2]
            request_host = request_host.split(':')[0]  # e.g. 'gzip.static.woot.com:9090'
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
            wait = entry["timings"]["wait"]	 # Not used
            receive = entry["timings"]["receive"]

            # Matching tstatdata
            for line in tstatdata:
                if line["httpid"] == http_id:
                    #fields_to_add = {'log'.decode('utf-8'): "null".decode('utf-8')}
                    fields_to_add = {unicode('session_url'): session_url, unicode('full_load_time'): unicode(onLoad),
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

                    line.update(fields_to_add)
                #print line
            #  End for cicle (matching tstatdata)

        json_data.close()
    except:
        pass

    print "from tstat ", httpid_from_tstat, len(httpid_from_tstat)
    print "from har ", httpid_from_har, len(httpid_from_har)
    missing_from_tstat = [x for x in httpid_from_har if x not in httpid_from_tstat]
    print missing_from_tstat
    return tstatdata

'''
if __name__ == '__main__':
    import os
    harfile = '/tmp/phantomjs.har'
    p = Parser(None, harfile, 1608989979)
    p.parse()
    #p.parseTstat()
    #p.parseHar()
    #p.merge_to_process()
    #arr = parseTstat(tstatfile, '\n', 1608989979)
    #rows = []
    #for line in arr:
    #    try:
    #        jsonstring = json.loads(line)
    #        rows.append(jsonstring)
    #    except ValueError:
    #        pass  # EOF
    #full_rows = updatebyHar(rows, harfile)
    #add_missing_from_har(full_rows, harfile)
