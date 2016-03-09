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
#
import logging
import json
from .cusum import Cusum
from collections import OrderedDict

logger = logging.getLogger('LocalDiagnosisManager')


class LocalDiagnosisManager():
    def __init__(self, db, url):
        self.url = url
        self.db = db
        self.table_passive_th = self.db.tables['passive_thresholds']
        self.table_cusum_th = self.db.tables['cusum_th']
        self.table_result = self.db.tables['diag_result']
        self.cusums = {}

    def get_passive_thresholds(self):
        q = '''select flt, http, tcp, dim, count
        from {0} where url like '%{1}%' '''.format(self.table_passive_th, self.url)
        try:
            res = self.db.execute(q)
            data = res[0]
            return {'full_load_time_th': data[0], 'http_th': data[1], 'tcp_th': data[2], 'dim_th': data[3],
                    'count': data[4]}
        except:
            return None

    def get_passive_data(self, sid):
        pkeys = ['sid', 'session_url', 'session_start', 'server_ip', 'full_load_time',
                 'page_dim', 'cpu_percent', 'mem_percent', 'is_sent']
        d = {}
        q = "select {0} from {1} where sid = {2} and session_url like '%{3}%'".format(','.join(pkeys), self.db.tables['aggr_sum'], sid, self.url)
        res = self.db.execute(q)
        for idx, el in enumerate(res[0]):
            d[pkeys[idx]] = el
        return d

    def get_active_data(self, sid):
        akeys = ['sid', 'session_url', 'ip_dest', 'remote_ip', 'ping', 'trace']
        a = []
        q = "select {0} from {1} where sid = {2} and session_url like '%{3}%'".format(','.join(akeys), self.db.tables['active'], sid, self.url)
        res = self.db.execute(q)
        for row in res:
            d = {}
            for idx, el in enumerate(row):
                try:
                    d[akeys[idx]] = json.loads(el)
                except (TypeError, ValueError):
                    d[akeys[idx]] = el
            a.append(d)
        return a

    def get_browser_data(self, sid):
        bkeys = ['sid', 'base_url', 'ip', 'netw_bytes', 'nr_obj', 'sum_syn', 'sum_http', 'sum_rcv_time']
        b = []
        q = "select {0} from {1} where sid = {2}".format(','.join(bkeys), self.db.tables['aggr_det'], sid)
        res = self.db.execute(q)
        for row in res:
            d = {}
            for idx, el in enumerate(row):
                d[bkeys[idx]] = el
            b.append(d)
        return b

    def get_cusums(self, url):
        names = ['cusumT1', 'cusumD1', 'cusumD2', 'cusumDH']
        q = '''select {0} from {1} where url = \'{2}\''''.format(','.join(names), self.table_cusum_th, url)
        res = self.db.execute(q)
        if len(res) == 0:
            logger.info("No cusums available for url {}: creating new ones.".format(url))
            return None
        if len(res) > 1:
            logger.warning("Got multiple values for url {}".format(url))
        row = res[0]
        for idx, el in enumerate(row):
            dic = json.loads(el)
            self.cusums[names[idx]] = Cusum(name=dic['name'], th=dic['th'],
                                            value=dic['cusum'], mean=dic['mean'], var=dic['var'],
                                            count=dic['count'])
        return self.cusums

    def update_cusums(self, url, first=False):
        keys = list(self.cusums.keys())
        d = dict.fromkeys(keys, None)
        for k in keys:
            d[k] = self.cusums[k].__dict__
        if first:
            q = '''insert into {0} (url, {1}) values ('{2}','''.format(self.table_cusum_th, ','.join(keys), url)
            q += ','.join("'" + json.dumps(d[k]) + "'" for k in keys) + ')'
            self.db.execute(q)
        else:
            for k, v in self.cusums.items():
                q = '''update {0} set {1} = {2} where url = '{3}' '''.format(self.table_cusum_th, k, "'" + json.dumps(d[k]) + "'", url)
                self.db.execute(q)
        logger.info("Cusum table updated.")

    def prepare_for_diagnosis(self, sid):
        TRAINING = 100
        if not sid:
            logger.error("sid not specified. Unable to run diagnosis.")
            return
        passive = self.get_passive_data(sid)
        active = self.get_active_data(sid)
        browser = self.get_browser_data(sid)

        locals = self.get_passive_thresholds()
        if self.get_cusums(passive['session_url']):
            logger.info("Cusums loaded")
        # get current data for cusum update

        trace = [x['trace'] for x in active if x['trace'] is not None][0]
        h1 = [x for x in trace if x['hop_nr'] == 1][0]['rtt']['max']
        h2 = [x for x in trace if x['hop_nr'] == 2][0]['rtt']['max']
        h3 = [x for x in trace if x['hop_nr'] == 3][0]['rtt']['max']
        http_time = sum([x['sum_http'] for x in browser])
        tcp_time = sum([x['sum_syn'] for x in browser])

        if not locals:
            logger.warning("First time hitting {0}: using current values.".format(self.url))
            time_th = passive['full_load_time'] + 1000
            http_th = http_time + 50
            tcp_th = tcp_time + 50
            dim_th = passive['page_dim'] + 5000
            #rcv_th = sum([x['sum_rcv_time'] for x in browser]) + 50  # TODO add rcv_th to local_diag
            self.insert_first_locals(time_th, http_th, tcp_th, dim_th)
        else:
            time_th = locals['full_load_time_th']
            dim_th = locals['dim_th']
            http_th = locals['http_th']
            tcp_th = locals['tcp_th']

         # TODO: find a way to have threshold setting
        t1 = h1 + 0.1
        d1 = h2 - h1 + 0.2
        try:       # TODO quick fix for hop3 not responding to ping (None)
            d2 = h3 - h2 + 0.3
        except TypeError:
            d2 = d1 + 0.3
        dh = http_time - tcp_time + 0.5

        if not self.cusums:
            self.cusums['cusumT1'] = Cusum(name='cusumT1', th=h1, value=h1)
            self.cusums['cusumD1'] = Cusum(name='cusumD1', th=d1, value=d1)
            self.cusums['cusumD2'] = Cusum(name='cusumD2', th=d2, value=d2)
            self.cusums['cusumDH'] = Cusum(name='cusumDH', th=dh, value=dh)
            self.update_cusums(passive['session_url'], first=True)
        else:
            if self.cusums['cusumT1'].get_count() < TRAINING:
                self.cusums['cusumT1'].compute(h1)
            if self.cusums['cusumD1'].get_count() < TRAINING:
                self.cusums['cusumD1'].compute(d1)
            if self.cusums['cusumD2'].get_count() < TRAINING:
                self.cusums['cusumD2'].compute(d2)
            if self.cusums['cusumDH'].get_count() < TRAINING:
                self.cusums['cusumDH'].compute(http_time - tcp_time)
            self.update_cusums(passive['session_url'])

        mem_th = cpu_th = 50

        passive_thresholds = {'time_th': time_th,
                              'dim_th': dim_th,
                              'http_th': http_th,
                              'tcp_th': tcp_th,
                              'mem_th': mem_th,
                              'cpu_th': cpu_th}

        return passive, active, browser, passive_thresholds

    def run_diagnosis(self, sid):
        diagnosis = OrderedDict({'sid': sid, 'result': None, 'details': None})
        passive_m, active_m, browser_m, passive_thresholds = self.prepare_for_diagnosis(sid)

        if not passive_m or not active_m or not browser_m or not passive_thresholds:
            diagnosis['result'] = 'Error'
            diagnosis['details'] = 'Unable to retrieve data'
            return diagnosis

        if passive_m['full_load_time'] < passive_thresholds['time_th']:
            diagnosis['result'] = 'No problem found.'
            diagnosis['details'] = ''
        else:
            if passive_m['mem_percent'] > passive_thresholds['mem_th'] or passive_m['cpu_percent'] > passive_thresholds['cpu_th']:
                diagnosis['result'] = 'Client overloaded'
                diagnosis['details'] = "mem = {0}%, cpu = {1}%".format(passive_m['mem_percent'], passive_m['cpu_percent'])
                return diagnosis
            t_http = sum([x['sum_http'] for x in browser_m])
            t_tcp = sum([x['sum_syn'] for x in browser_m])
            if t_http < passive_thresholds['http_th']:
                if passive_m['page_dim'] > passive_thresholds['dim_th']:
                    diagnosis['result'] = 'Page too big'
                    diagnosis['details'] = "page_dim = {0} bytes".format(passive_m['page_dim'])
                elif t_tcp > passive_thresholds['tcp_th']:
                    diagnosis['result'] = 'Web server too far'
                    diagnosis['details'] = "sum_syn = {0} ms".format(t_tcp)
                else:
                    diagnosis['result'] = 'No problem found'
                    diagnosis['details'] = "Unable to get more details"
            else:
                diff = t_http - t_tcp
                diagnosis['result'], diagnosis['details'] = self._check_network(active_m, diff)

        q = "update {0} set count = count + 1 where url like '%{1}%'"\
            .format(self.table_passive_th, self.url)
        self.db.execute(q)
        logger.info(diagnosis)
        self.store_diagnosis_result(sid, diagnosis)
        return diagnosis

    def _check_network(self, active, diff):
        result = details = None
        trace = [x['trace'] for x in active if x['trace'] is not None][0]
        first_hop = [x for x in trace if x['hop_nr'] == 1][0]
        second_hop = [x for x in trace if x['hop_nr'] == 2][0]
        third_hop = [x for x in trace if x['hop_nr'] == 3][0]

        gw_addr = first_hop['ip_addr']
        gw_rtt = first_hop['rtt']['avg']
        second_addr = second_hop['ip_addr']
        second_rtt = second_hop['rtt']['avg']
        third_addr = third_hop['ip_addr']
        try:
            third_rtt = third_hop['rtt']['avg']
        except TypeError:
            third_rtt = second_rtt * 2  # FIXME: case when third hop does not answer ping

        if third_rtt is None:
            third_rtt = second_rtt * 2  # FIXME: case when third hop does not answer ping

        if self.cusums['cusumT1'].compute(gw_rtt):
            result = 'Local congestion (LAN/GW)'
            details = "cusum on RTT to 1st hop {0}".format(gw_addr)
        else:
            #d1 = [x-y for x, y in zip([second_rtt], [gw_rtt])]
            #d2 = [x-y for x, y in zip([third_rtt], [second_rtt])]
            d1 = second_rtt - gw_rtt
            d2 = third_rtt - second_rtt
            if self.cusums['cusumD1'].compute(d1):
                if self.cusums['cusumD2'].compute(d2):
                    result = 'Network congestion'
                    details = "cusum on Delta1 [{0},{1}] and Delta2 [{1},{2}]".format(gw_addr, second_addr, third_addr)
                else:
                    result = 'Gateway congestion'
                    details = "cusum on Delta1 [{0},{1}]".format(gw_addr, second_addr)

        if not result:
            if self.cusums['cusumDH'].compute(diff):
                result = 'Remote Web Server'
                details = "cusum on t_http - t_tcp"
            else:
                result = 'Network generic (far)'
                details = "Unable to get more details"

        return result, details

    def insert_first_locals(self, flt, http, tcp, dim):
        q = "insert into {0}(url, flt, http, tcp, dim, count) values "\
            .format(self.table_passive_th)
        q += "('{0}', {1}, {2}, {3}, {4}, 1)".format(self.url, flt, http, tcp, dim)
        self.db.execute(q)

    def store_diagnosis_result(self, sid, diagnosis):
        q = "select session_start, session_url from {0} where sid = {1}".\
            format(self.db.tables['aggr_sum'], sid)
        res = self.db.execute(q)
        when = res[0][0]
        url = res[0][1]
        q = "insert into {0} (sid, url, when_browsed, diagnosis) values".format(self.table_result)
        q += " ({0}, '{1}', '{2}', '{3}')".format(sid, url, when, json.dumps(diagnosis))
        self.db.execute(q)
        logger.debug("Diagnosis result saved.")
