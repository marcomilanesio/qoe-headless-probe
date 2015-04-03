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
import socket
import json
import logging
import collections

logger = logging.getLogger('JSONClient')


class JSONClient():
    def __init__(self, config, dbcli):
        #self.activetable = config.get_database_configuration()['activetable']
        #self.rawtable = config.get_database_configuration()['rawtable']
        #self.summarytable = config.get_database_configuration()['aggregatesummary']
        #self.detailtable = config.get_database_configuration()['aggregatedetails']
        #self.probeidtable = config.get_database_configuration()['probeidtable']
        #self.localdiagnosisresulttable = config.get_database_configuration()['localdiagnosisresult']
        self.srv_ip = config.get_jsonserver_configuration()['ip']
        self.srv_port = int(config.get_jsonserver_configuration()['port'])
        self.srv_mode = int(config.get_jsonserver_configuration()['mode'])
        self.json_file = config.get_flume_configuration()['outfile']
        self.db = dbcli
        self.probeid = self._get_client_id_from_db()

    def _get_client_id_from_db(self):
        q = "select distinct probe_id from {0}".format(self.db.tables['probe'])
        r = self.db.execute(q)
        assert len(r) == 1
        return int(r[0][0])

    def prepare_data(self):
        probedata = {}
        query = '''select user, probe_id, first_start, location from {0}'''.format(self.db.tables['probe'])
        res = self.db.execute(query)
        assert len(res) == 1
        user, probe_id, first_start, location_str = res[0]
        assert self.probeid == probe_id
        probedata.update({'user': user, 'probe_id': probe_id,
                          'first_start': first_start, 'location': json.loads(location_str)})

        query = '''select sid, session_url, session_start, server_ip,
        full_load_time, page_dim, cpu_percent, mem_percent from {0} where not is_sent'''.\
            format(self.db.tables['aggr_sum'])
        res = self.db.execute(query)
        if len(res) == 0:
            logger.warning("Nothing to send. All flags are valid.")
            return

        sessions_list = []
        for row in res:
            r = collections.OrderedDict()
            r['probeid'] = probedata
            r['sid'] = row[0]
            r['session_url'] = row[1]
            r['session_start'] = str(row[2])  # convert to string to be json-serializable
            r['server_ip'] = row[3]
            r['full_load_time'] = row[4]
            r['page_dim'] = row[5]
            r['cpu_percent'] = row[6]
            r['mem_percent'] = row[7]
            r['services'] = []
            r['active_measurements'] = {}
            r['local_diagnosis'] = {}

            query = '''select base_url, ip, netw_bytes, nr_obj, sum_syn, sum_http, sum_rcv_time
            from {0} where sid = {1}'''.format(self.db.tables['aggr_det'], row[0])
            det = self.db.execute(query)

            for det_row in det:
                d = collections.OrderedDict()
                d['base_url'] = det_row[0]
                d['ip'] = det_row[1]
                d['netw_bytes'] = det_row[2]
                d['nr_obj'] = det_row[3]
                d['sum_syn'] = det_row[4]
                d['sum_http'] = det_row[5]
                d['sum_rcv_time'] = det_row[6]
                r['services'].append(d)

            query = '''select remote_ip, ping, trace
            from {0} where sid = {1}'''.format(self.db.tables['active'], row[0])
            active = self.db.execute(query)

            for active_row in active:
                a = collections.OrderedDict()
                a[active_row[0]] = {'ping': active_row[1], 'trace': active_row[2]}
                r['active_measurements'].update(a)      # dictionary!

            query = '''select diagnosis from {0} where sid = {1} and url = '{2}' '''\
                .format(self.db.tables['diag_result'], r['sid'], r['session_url'])

            res = self.db.execute(query)
            r['local_diagnosis'] = json.loads(res[0][0])

            sessions_list.append(r)

        #j = json.dumps(sessions_list)
        return sessions_list

    def save_json_file(self, measurements):
        # measurements is a list of dictionaries
        # one for each session: ['passive', 'active', 'ts', 'clientid', 'sid']
        logger.info('Saving json file...')
        with open(self.json_file, 'w') as out:
            for m in measurements:
                json.dump(m, out)
            #out.write(measurements)
            #out.write(json.dumps([m for m in measurements]))
        return self.json_file

    def send_json_to_srv(self, measurements):
        logger.info('Contacting server...')
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.srv_ip, self.srv_port))
        except socket.error as e:
            logger.error('Socket error({0}): {1}'.format(e.errno, e.strerror))
            return False

        data = json.dumps([m for m in measurements])
        logger.info("Sending %d bytes" % len(data))
        s.sendall(data + '\n')
        result = json.loads(s.recv(1024))
        s.close()
        logger.info("Received %s" % str(result))

        for sid in result['sids']:
            q = '''update %s set is_sent = 1 where sid = %d''' % (self.db.tables['aggr_sum'], int(sid))
            self.db.execute(q)
        logger.debug("Set is_sent flag on summary table for sids {0}.".format(result['sids']))

        return True
