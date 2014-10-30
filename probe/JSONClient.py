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
import numpy
from DBClient import DBClient
from Configuration import Configuration 
from LocalDiagnosisManager import LocalDiagnosisManager
import logging
import time

logger = logging.getLogger('JSONClient')


class JSONClient():
    def __init__(self, config):
        self.activetable = config.get_database_configuration()['activetable']
        self.rawtable = config.get_database_configuration()['rawtable']
        self.srv_ip = config.get_jsonserver_configuration()['ip']
        self.srv_port = int(config.get_jsonserver_configuration()['port'])
        self.srv_mode = int(config.get_jsonserver_configuration()['mode'])
        self.json_file = ".toflume/data_to_send.json"
        self.db = DBClient(config)
        self.probeid = self._get_client_id_from_db()
    
    def _get_client_id_from_db(self):
        q = 'select distinct on (probe_id) probe_id from %s ' % self.rawtable
        r = self.db.execute_query(q)
        assert len(r) == 1
        return int(r[0][0])
        
    def prepare_data(self):
        query = 'select * from %s where not sent' % self.activetable
        res = self.db.execute_query(query)
        sids = list(set([r[1] for r in res]))
        if len(sids) == 0:
            logger.info('Nothing to send (all sent flags are valid). Returning...')
            return
        logger.debug('Found %d stored sessions to send... ' % len(sids))
        sent_sids = []
        local_stats = self._prepare_local_data(sids)
        local_data = {'clientid': self.probeid, 'local': local_stats}
        str_to_send = "local: " + json.dumps(local_data)
        logger.debug('str_to_send %s' % str_to_send)
        measurements = []
        for sid in sids:
            measurements.append({'clientid': self.probeid, 'sid': str(sid),
                                 'ts': local_stats[str(sid)]['start'], 'passive': local_stats[str(sid)], 'active': []})

        for row in res:
            active_data = {'clientid': self.probeid, 'ping': None, 'trace': []}
            count = 0
            sid = int(row[1])
            session_url = row[2]
            remoteaddress = row[3]
            ping = json.loads(row[4])
            trace = json.loads(row[5])

            active_data['ping'] = {'sid': sid, 'session_url': session_url, 'remoteaddress': remoteaddress,
                                   'min': ping['min'], 'max': ping['max'], 'avg': ping['avg'], 'std': ping['std'],
                                   'loss': ping['loss'], 'host': ping['host']}

            for step in trace:
                if len(step) > 1:
                    empty_targets = [t for t in step if t[0] == '???' or t[1] == []]
                    for empty in empty_targets:
                        step.remove(empty)
                        count += 1

            for step in trace:
                step_nr = step['hop_nr']
                step_addr = step['ip_addr']
                step_rtt = step['rtt']
                step_alias = step['endpoints']
                '''
                @TODO
                Consider different endpoints
                '''
                active_data['trace'].append({'sid': sid, 'remoteaddress': remoteaddress, 'step': step_nr,
                                             'step_address': step_addr, 'rtt': step_rtt})

            for session in measurements:
                if int(session['sid']) == sid:
                    session['active'].append(active_data)

        # measurements is a list of dictionaries
        # one for each session: ['passive', 'active', 'ts', 'clientid', 'sid']
        return measurements

    def save_json_file(self, measurements):
        # measurements is a list of dictionaries
        # one for each session: ['passive', 'active', 'ts', 'clientid', 'sid']
        logger.info('Saving json file...')
        with open(self.json_file, 'w') as out:
            out.write(json.dumps([m for m in measurements]))
        return self.json_file

    def send_json_to_srv(self, measurements):
        logger.info('Contacting server...')
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.srv_ip, self.srv_port))
        except socket.error as e:
            logger.error('Socket error({0}): {1}'.format(e.errno, e.strerror))
            return

        data = json.dumps([m for m in measurements])
        logger.info("Sending %d bytes" % len(data))
        s.sendall(data + '\n')
        result = json.loads(s.recv(1024))
        s.close()
        logger.info("Received %s" % str(result))
        return self.save_result(result)

    def save_result(self, result):
        received_sids = result['sids']
        if len(received_sids) > 0:
            for sent_sid in received_sids:
                update_query = '''update %s set sent = 't' where sid = %d''' % (self.activetable, int(sent_sid))
                self.db.execute_update(update_query)
                logger.info('updated sent sid on %s' % self.activetable)
        else:
            logger.warning('Unable to send anything to server.')
        return result

    def _prepare_local_data(self, sids):
        logger.debug('calling LocalDiagnosisManager for {0}'.format(self.probeid))
        l = LocalDiagnosisManager(self.db, self.probeid, sids)
        logger.debug('Got {0}'.format(type(l)))
        return l.do_local_diagnosis()

    def send_request_for_diagnosis(self, url, time_range=6):
        data = {'clientid': self.probeid, 'url': url, 'time_range': time_range}
        str_to_send = 'check: ' + json.dumps(data)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.srv_ip, self.srv_port))
        s.send(str_to_send + "\n")
        result = json.loads(s.recv(1024))
        s.close()
        return result

