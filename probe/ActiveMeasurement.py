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

import subprocess
import json
import re
import logging
import numpy

logger = logging.getLogger('Active')


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
        #rttmin = rttavg = rttmax = rttmdev = -1.0
        #out_ping = out.strip().split('\n')[-1].split(' = ')
        #m = re.search('--- %s ping statistics ---' % self.target, out)
        #if len(out_ping) > 1:
        #    res = out_ping[1].split()[0]
        #    try:
        #        rttmin, rttavg, rttmax, rttmdev = map(float, res.strip().split("/"))
        #        logger.debug('rtts - %.3f, %.3f, %.3f, %.3f' % (rttmin, rttavg, rttmax, rttmdev))
        #    except ValueError:
        #        logger.error('Unable to map float in do_ping [%s]' % out.strip())
        #self.result = json.dumps({'min': rttmin, 'max': rttmax, 'avg': rttavg, 'std': rttmdev})

        res = {}
        try:
            res = self.parse(out)
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

    @staticmethod
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


class TracerouteIcmp(Traceroute):
    def __init__(self, script, host, maxttl=32):
        Traceroute.__init__(self, host, maxttl)
        self.cmd = "%s %s %d" % (script, host, maxttl)
        self.out = ''
        self.err = ''
        self.result = None

    def run_in_memory(self):
        traceroute = subprocess.Popen(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        self.out, self.err = traceroute.communicate()
        if not self.err:
            self.parse_result()
            if self.result:
                return True
        return False

    def parse_result(self):
        result = []
        f = self.out.strip().split("\n")
        for el in f:
            if re.match('traceroute', el):
                continue
            else:
                hop = [x for x in el.strip().split() if x != 'ms']
                thop = TracerouteHop(int(hop[0]))
                thop.add_measurement(hop[1:])
                result.append(thop.__dict__)
        self.result = json.dumps(result)


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
            self.rtt = -1
            return

        if len(_endpoints) > 1:  # more endpoints
            self.endpoints = _endpoints[1:]
        self.ip_addr = _endpoints[0]

        clean = [x for x in arr_data if x not in _endpoints and x != '*' and x != '!X']

        if len(clean) > 0:
            self.rtt['min'] = min(map(float, clean))
            self.rtt['max'] = max(map(float, clean))
            self.rtt['avg'] = numpy.mean(map(float, clean))
            self.rtt['std'] = numpy.std(map(float, clean))
        else:
            self.rtt = -1

    def __str__(self):
        return '%d: %s, %.3f %s' % (self.hop_nr, self.ip_addr, self.rtt['avg'], str(self.endpoints))


class Monitor(object):
    def __init__(self, config, dbconn):
        self.config = config
        self.db = dbconn
        self.inserted_sid = self.db.get_inserted_sid_addresses()
        logger.info('Started active monitor: %d session(s).' % len(self.inserted_sid))

    def run_active_measurement(self):
        tot = {}
        probed_ip = {}
        for sid, dic in self.inserted_sid.iteritems():
            to_insert = []
            url = dic['url']
            server_ip = dic['complete']
            ip_addrs = list(set(dic['addresses']))  # remove possible duplicates

            if sid not in tot.keys():
                tot[sid] = []

            if server_ip not in probed_ip.keys():
                logger.debug("Running ping for {0}".format(server_ip))
                ping = Ping(server_ip)
                ping.run()
                ping_result = ping.get_result()
                logger.debug("Running traceroute for {0}".format(server_ip))
                trace = Traceroute(server_ip)
                trace.run()
                trace_result = trace.get_result()

                to_insert.append({'url': url, 'ip': server_ip, 'ping': ping_result, 'trace': trace_result})
                #tot[sid].append({'url': url, 'ip': server_ip, 'ping': ping_result, 'trace': trace_result})
                probed_ip[server_ip] = {'ping': ping_result, 'trace': trace_result}

                for k, v in trace.get_first_hops().iteritems():
                    if v != 'n.a.':
                        p = Ping(v)
                        p.run()
                        p_result = p.get_result()
                        probed_ip[v] = {'ping': p_result}
                        to_insert.append({'url': url, 'ip': v, 'ping': p_result})
                    else:
                        logger.warning("Hop {0} [{1}] not answering to ping".format(k, v))

            else:
                to_insert.append({'url': url, 'ip': server_ip, 'ping': probed_ip[server_ip]['ping'],
                                  'trace': probed_ip[server_ip]['trace']})
                #tot[sid].append({'url': url, 'ip': server_ip, 'ping': probed_ip[server_ip]['ping'],
                #                 'trace': probed_ip[server_ip]['trace']})

            res, done = self.check_ipaddrs(url, ip_addrs, probed_ip)
            for k, v in done.iteritems():
                probed_ip[k] = v
            for d in res:
                to_insert.append(d)
                #tot[sid].append(d)

            logger.info("Computed Active Measurement for {0} [{1}] in session {2}".format(url, server_ip, sid))

            #self.db.insert_active_measurement(sid, server_ip, tot)
            self.db.insert_active_measurement(sid, server_ip, to_insert)
        logger.info('ping and traceroute saved into db.')

    def check_ipaddrs(self, url, ip_addrs, probed_ip):
        res = []
        done = {}
        for ip in ip_addrs:
            if ip not in probed_ip.keys():
                ping = Ping(ip)
                logger.debug("Running ping for ip : {0}".format(ip))
                ping.run()
                res.append({'url': url, 'ip': ip, 'ping': ping.get_result()})
                done[ip] = {'ping': ping}
            else:
                res.append({'url': url, 'ip': ip, 'ping': probed_ip[ip]['ping']})
        return res, done

    def do_measure(self, ip_dest):
        for sid, dic in self.inserted_sid.iteritems():
            logger.debug("Session {0} to url {1}: resolved {2}, objects from found {3}".format(sid, dic['url'],
                                                                                               ip_dest, dic['address']))
        tot = {}
        print (self.inserted_sid)
        # TODO: more logic here. e.g., if nr_obj is big
        for sid, dic in self.inserted_sid.iteritems():
            if sid not in tot.keys():
                tot[sid] = []
            for ip in dic['address']:
                traceicmp = TracerouteIcmp(self.config.get_traceroute_script(), ip)
                traceicmp.run()
                ping = Ping(ip)
                ping.run()
                tot[sid].append({'url': dic['url'], 'ip': ip,
                                 'ping': ping.get_result(), 'trace': traceicmp.get_result()})
                logger.info("Computed Active Measurement for {0} in session {1}".format(ip, sid))

        self.db.insert_active_measurement(ip_dest, tot)
        logger.info('ping and traceroute saved into db.')

    # one single traceroute
    # ping to step 1,2,3, dest
    # to feed the diagnosis
    def do_single_measure(self, ip_dest):
        tot = {}
        traceicmp = TracerouteIcmp(self.config.get_traceroute_script(), ip_dest)
        if traceicmp.run_in_memory():
            print traceicmp.get_result()


if __name__ == '__main__':
    from Configuration import Configuration
    c = Configuration('probe.conf')
    m = Monitor(c)
    m.do_single_measure('213.92.16.191')
