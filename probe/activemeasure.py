#!/usr/bin/python
#
# mPlane QoE Probe
#
# (c) 2013-2014 mPlane Consortium (http://www.ict-mplane.eu)
#               Author: Marco Milanesio <milanesio.marco@gmail.com>
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

#import subprocess
import json
#import re
import logging
#import numpy
#import os

logger = logging.getLogger('Active')

'''
class Measure(object):
    def __init__(self, host):
        self.target = host
        self.result = {}
        self.cmd = ''

    def get_result(self):
        return self.result

    def get_cmd(self):
        return self.cmd


class Ping(Measure):
    def __init__(self, host):
        Measure.__init__(self, host)
        self.cmd = 'ping -c 5 %s ' % self.target

    def run(self):
        ping = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, error = ping.communicate()
        res = {}
        try:
            res = self.parse(out.decode("utf-8"))
            logger.info('Ping received. {0}'.format(res))
        except Exception:
            logger.error('Unable to receive valid ping values')

        self.result = json.dumps(res)

    def parse(self, ping_output):
        matcher = re.compile(r'PING ([a-zA-Z0-9.\-]+) \(')
        host = Ping._get_match_groups(ping_output, matcher)[0]
        matcher = re.compile(r'(\d+) packets transmitted, (\d+) received, (\d+)% packet loss, ')
        sent, received, loss = map(int, Ping._get_match_groups(ping_output, matcher))

        try:
            matcher = re.compile(r'(\d+.\d+)/(\d+.\d+)/(\d+.\d+)/(\d+.\d+)')
            rttmin, rttavg, rttmax, rttmdev = map(float, Ping._get_match_groups(ping_output, matcher))
        except:
            rttmin, rttavg, rttmax, rttmdev = [-1]*4

        return {'host': host, 'sent': sent, 'received': received, 'loss': loss, 'min': rttmin,
                'avg': rttavg, 'max': rttmax, 'std': rttmdev}

    def _get_match_groups(ping_output, regex):
        match = regex.search(ping_output)
        if not match:
            raise Exception('Invalid PING output:\n' + ping_output)
        return match.groups()


class Traceroute(Measure):
    HEADER_REGEXP = re.compile(r'traceroute to (\S+) \((\d+\.\d+\.\d+\.\d+)\)')

    def __init__(self, host, maxttl=32):
        Measure.__init__(self, host)
        self.cmd = 'traceroute -n -m %d %s ' % (maxttl, self.target)
        self.result = None

    def run(self):
        fname = self.target + '.traceroute'
        outfile = open(fname, 'w')
        traceroute = subprocess.Popen(self.cmd, stdout=outfile, stderr=subprocess.PIPE, shell=True)
        _,  err = traceroute.communicate()
        if err:
            logger.error('Error in %s' % self.cmd)
        else:
            logger.debug('Traceroute computed.')
        outfile.close()
        self.parse_file(fname)

    def parse_file(self, outfile):
        f = open(outfile, 'r')
        arr = f.readlines()
        f.close()
        result = []
        for line in arr:
            if self.HEADER_REGEXP.match(line):  # header
                continue
            else:
                hop = Traceroute._parse_line(line)
                result.append(hop.__dict__)
        self.result = json.dumps(result)

    @staticmethod
    def _parse_line(line):
        hop = line.split()
        hop_nr = hop[0]
        hop.pop(0)
        remains = [x for x in hop if x != 'ms']
        t_hop = TracerouteHop(hop_nr)
        t_hop.add_measurement(remains)
        return t_hop

    def get_result(self):
        return self.result

    def get_first_hops(self, hop_limit=3):
        res = {}
        for hop in json.loads(self.result):
            if hop['hop_nr'] <= hop_limit:
                res[hop['hop_nr']] = hop['ip_addr']
        return res


class TracerouteHop(object):
    IPADDR_REGEXP = re.compile(r'\d+\.\d+\.\d+\.\d+')

    def __init__(self, hop_nr):
        self.hop_nr = int(hop_nr)
        self.ip_addr = None
        self.rtt = {'min': 0.0, 'max': 0.0, 'avg': 0.0, 'std': 0.0}
        self.endpoints = []

    def add_measurement(self, arr_data):
        _endpoints = [x for x in arr_data if self.IPADDR_REGEXP.match(x)]
        if len(_endpoints) == 0:  # no ip returned (3 packet drops)
            self.ip_addr = 'n.a.'
            for k, _ in self.rtt.items():
                self.rtt[k] = None
            return

        if len(_endpoints) > 1:  # more endpoints
            self.endpoints = _endpoints[1:]
        self.ip_addr = _endpoints[0]

        clean = [x for x in arr_data if x not in _endpoints and x != '*' and x != '!X']

        if len(clean) > 0:
            cl = list(map(float, clean))
            self.rtt['min'] = min(cl)
            self.rtt['max'] = max(cl)
            self.rtt['avg'] = numpy.mean(cl)
            self.rtt['std'] = numpy.std(cl)
        else:
            self.rtt['min'] = None
            self.rtt['max'] = None
            self.rtt['avg'] = None
            self.rtt['std'] = None

    def __str__(self):
        return '%d: %s, %.3f %s' % (self.hop_nr, self.ip_addr, self.rtt['avg'], str(self.endpoints))
'''

import concurrent.futures
import time
import subprocess
import re
import numpy


def run_traceroute(dest, ttl=32):
    trace = []
    HEADER_REGEXP = re.compile(r'traceroute to (\S+) \((\d+\.\d+\.\d+\.\d+)\)')
    IPADDR_REGEXP = re.compile(r'\d+\.\d+\.\d+\.\d+')
    RTT_REGEXP = re.compile(r'\d+.\d+')

    rtt = {'min': 0.0, 'max': 0.0, 'avg': 0.0, 'std': 0.0}

    cmd = "traceroute -n -m {0} {1}".format(ttl, dest)
    out, err = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).communicate()
    if err:
        print("Error: ", err)
    for line in out.decode("utf-8").split("\n"):
        if HEADER_REGEXP.match(line):  # header
            continue
        else:
            hop = [x for x in line.split(" ") if x is not '']
            if len(hop) == 0:
                continue
            else:
                hop_nr = int(hop[0])
                ip_addr = hop[1] if IPADDR_REGEXP.match(hop[1]) else 'n.a.'
                if ip_addr == 'n.a.':
                    for k in rtt.keys():
                        rtt[k] = -1
                else:
                    latencies = list(map(float, [x for x in hop[2:] if RTT_REGEXP.match(x) and not IPADDR_REGEXP.match(x)]))
                    rtt['min'] = min(latencies)
                    rtt['max'] = max(latencies)
                    rtt['avg'] = numpy.mean(latencies)
                    rtt['std'] = numpy.std(latencies)
            trace.append({'hop_nr': hop_nr, 'ip_addr': ip_addr, 'rtt': rtt})
    return trace


def get_match_groups(ping_output, regex):
    match = regex.search(ping_output)
    if not match:
        raise Exception('Invalid PING output:\n' + ping_output)
    return match.groups()


def parse(ping_output):
    po = ping_output.decode("utf-8")
    matcher = re.compile(r'PING ([a-zA-Z0-9.\-]+) \(')
    host = get_match_groups(po, matcher)[0]
    matcher = re.compile(r'(\d+) packets transmitted, (\d+) received, (\d+)% packet loss, ')
    try:
        sent, received, loss = map(int, get_match_groups(po, matcher))
    except Exception as e:
        logger.error(e)
        return {'host': host, 'sent': -1, 'received': -1, 'loss': -1, 'min': -1,
            'avg': -1, 'max': -1, 'std': -1}

    try:
        matcher = re.compile(r'(\d+.\d+)/(\d+.\d+)/(\d+.\d+)/(\d+.\d+)')
        rttmin, rttavg, rttmax, rttmdev = map(float, get_match_groups(po, matcher))
    except:
        rttmin, rttavg, rttmax, rttmdev = [-1]*4

    return {'host': host, 'sent': sent, 'received': received, 'loss': loss, 'min': rttmin,
            'avg': rttavg, 'max': rttmax, 'std': rttmdev}


def run_ping(dest):
    ping = subprocess.Popen("ping -c 5 {}".format(dest), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, error = ping.communicate()
    if error:
        return "Error"
    return parse(out)


class ActiveMonitor(object):
    def __init__(self, config, dbconn):
        self.config = config
        self.db = dbconn
        self.inserted_sid = self.db.get_inserted_sid_addresses()
        logger.debug("inserted.sid: ", self.inserted_sid)
        logger.info('Started active monitor: %d session(s).' % len(self.inserted_sid))

    def run_active_measurement(self):
        tot = {}
        result = {}
        probed_ip = {}
        for sid, dic in self.inserted_sid.items():
            logger.debug("sid, dic: {0} {1}".format(sid, dic))
            to_insert = []
            url = dic['url']
            server_ip = dic['server_ip']
            try:
                ip_addrs = list(set(dic['addresses']))  # remove possible duplicates
            except KeyError:
                logger.error("No ip addresses fetched when browsing {0} ({1}): ['addresses']: {2}".format(server_ip, url, dic.keys()))
                ip_addrs = []

            if sid not in tot.keys():
                tot[sid] = []

            if server_ip not in probed_ip.keys():
                logger.debug("Running ping for {0}".format(server_ip))
                ping = run_ping(server_ip)
                logger.debug("Running traceroute for {0}".format(server_ip))
                trace = run_traceroute(server_ip)

                # first three hops
                addr = [x['ip_addr'] for x in trace if x['hop_nr'] < 4]

                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                    for ip, res in zip(addr, executor.map(run_ping, addr)):
                        p_result = json.dumps(res)
                        probed_ip[ip] = {'ping': p_result}
                        to_insert.append({'url': url, 'ip': ip, 'ping': p_result})

                ping_result = json.dumps(ping)
                trace_result = json.dumps(trace)
                #ping = Ping(server_ip)
                #ping.run()
                #ping_result = ping.get_result()
                #logger.debug("Running traceroute for {0}".format(server_ip))
                #trace = Traceroute(server_ip)
                #trace.run()
                #trace_result = trace.get_result()

                to_insert.append({'url': url, 'ip': server_ip, 'ping': ping_result, 'trace': trace_result})
                #tot[sid].append({'url': url, 'ip': server_ip, 'ping': ping_result, 'trace': trace_result})
                probed_ip[server_ip] = {'ping': ping_result, 'trace': trace_result}
                '''
                for k, v in trace.get_first_hops().items():
                    if v != 'n.a.':
                        p = Ping(v)
                        p.run()
                        p_result = p.get_result()
                        probed_ip[v] = {'ping': p_result}
                        to_insert.append({'url': url, 'ip': v, 'ping': p_result})
                    else:
                        logger.warning("Hop {0} [{1}] not answering to ping".format(k, v))
                '''
            else:
                to_insert.append({'url': url, 'ip': server_ip, 'ping': probed_ip[server_ip]['ping'],
                                  'trace': probed_ip[server_ip]['trace']})

            res, done = self.check_ipaddrs(url, ip_addrs, probed_ip)
            for k, v in done.items():
                probed_ip[k] = v
            for d in res:
                to_insert.append(d)

            logger.info("Computed Active Measurement for {0} [{1}] in session {2}".format(url, server_ip, sid))

            self.db.insert_active_measurement(sid, server_ip, to_insert)
            result[sid] = to_insert
        logger.info('ping and traceroute saved into db.')
        return result

    def check_ipaddrs(self, url, ip_addrs, probed_ip):
        res = []
        done = {}
        for ip in ip_addrs:
            if ip not in probed_ip.keys():
                logger.debug("Running ping for ip : {0}".format(ip))
                ping = run_ping(ip)
                res.append({'url': url, 'ip': ip, 'ping': json.dumps(ping)})
                done[ip] = {'ping': ping}
            else:
                res.append({'url': url, 'ip': ip, 'ping': probed_ip[ip]['ping']})
        return res, done
