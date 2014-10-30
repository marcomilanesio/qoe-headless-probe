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
import psycopg2
import Utils
import sys
from Configuration import Configuration
import logging
import fpformat
import random
import datetime
import time
import json

logger = logging.getLogger('DBClient')


class DBClient:
    def __init__(self, config):
        #connect to the database
        self.dbconfig = config.get_database_configuration()
        try:
            self.conn = psycopg2.connect(database=self.dbconfig['dbname'], user=self.dbconfig['dbuser'])
            logger.debug('DB connection established')
        except psycopg2.DatabaseError, e:
            print 'Unable to connect to DB. Error %s' % e
            logger.error('Unable to connect to DB. Error %s' % e)
            sys.exit(1)

    def create_tables(self):
        self.create_plugin_table()
        self.create_activemeasurement_table()
        self.create_aggregate_tables()

    def create_aggregate_tables(self):
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
            row_id SERIAL,
            sid INT,
            session_url TEXT,
            session_start TIMESTAMP,
            server_ip TEXT,
            full_load_time INT,
            page_dim INT,
            cpu_percent INT,
            mem_percent INT,
            is_sent BOOLEAN,
            PRIMARY KEY (sid, session_start)
        ) ''' % self.dbconfig['aggregatesummary'])

        cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
            sid INT,
            base_url TEXT,
            ip TEXT,
            netw_bytes INT,
            nr_obj INT,
            sum_syn INT,
            sum_http INT,
            sum_rcv_time INT,
            PRIMARY KEY (sid, ip)
        ) ''' % self.dbconfig['aggregatedetails'])

        self.conn.commit()

    def create_idtable(self):
        cursor = self.conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS client_id (probe_id INT4, first_start TEXT)''')
        self.conn.commit()

    def get_clientID(self):
        client_id = 0
        query = "SELECT probe_id FROM client_id"
        res = self.execute_query(query)
        if res != []:
            client_id = int(res[0][0])
        else:
            client_id = self.create_clientID()
        return client_id

    def create_clientID(self):
        cursor = self.conn.cursor()
        client_id = fpformat.fix(random.random()*2147483647, 0)		# int4 range in PgSQL: -2147483648 to +2147483647
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        state = '''INSERT INTO client_id VALUES ('%s', '%s')'''% (client_id, st)
        cursor.execute(state)
        self.conn.commit()
        return client_id

    def create_plugin_table(self):
        #create a Table for the Firefox plugin
        cursor = self.conn.cursor()

        cursor.execute('''CREATE TABLE IF NOT EXISTS %s (
        row_id SERIAL,
        uri TEXT,
        host TEXT,
        request_ts TIMESTAMP,
        content_type TEXT,
        content_len INT,
        keep_alive BOOLEAN,
        httpid INT,
        session_start TIMESTAMP,
        session_url TEXT,
        cache INT,
        local_ip TEXT,
        local_port INT,
        remote_ip TEXT,
        remote_port INT,
        response_code INT,
        get_bytes INT,
        header_bytes INT,
        body_bytes INT,
        cache_bytes INT,
        dns_start TIMESTAMP,
        dns_time INT,
        syn_start TIMESTAMP,
        syn_time INT,
        get_sent_ts TIMESTAMP,
        first_bytes_rcv TIMESTAMP,
        app_rtt INT,
        end_time TIMESTAMP,
        rcv_time INT,
        full_load_time INT,
        annoy INT,
        tab_id TEXT,
        cpu_percent INT,
        mem_percent INT,
        ping_gateway TEXT,
        ping_google TEXT,
        probe_id INT,
        sid INT,
        is_sent BOOLEAN
        )
        ''' % self.dbconfig['rawtable'])
        self.conn.commit()

    def create_activemeasurement_table(self):
        cursor = self.conn.cursor()
        # PSQL > 9.2 change TEXT to JSON
        cursor.execute('''CREATE TABLE IF NOT EXISTS %s (ip_dest INET, sid INT8, session_url TEXT,
        remote_ip INET, ping TEXT, trace TEXT, sent BOOLEAN)''' % self.dbconfig['activetable'])
        self.conn.commit()

    @staticmethod
    def _unicode_to_ascii(item):
        return item.encode('ascii', 'ignore')

    @staticmethod
    def _convert_to_ascii(arr):
        res = []
        for i in arr:
            res.append(DBClient._unicode_to_ascii(i))
        return res

    def write_plugin_into_db(self, datalist, stats):
        #read json objects from each line of the plugin file
        cursor = self.conn.cursor()
        table_name = self.dbconfig['rawtable']
        insert_query = 'INSERT INTO ' + table_name + ' (%s) values %r RETURNING row_id'
        update_query = 'UPDATE ' + table_name + ' SET mem_percent = %s, cpu_percent = %s where row_id = %d'
        for obj in datalist:
            #print obj
            if obj.has_key("session_url"):
                url = DBClient._unicode_to_ascii(obj['session_url'])
                cols = ', '.join(obj)
                to_execute = insert_query % (cols, tuple(DBClient._convert_to_ascii(obj.values())))
                #logger.debug('to_execute: %s' % to_execute)
                try:
                    cursor.execute(to_execute)
                    row_id = cursor.fetchone()[0]
                    to_update = update_query % (stats[url]['mem'], stats[url]['cpu'], row_id)
                    cursor.execute(to_update)
                    self.conn.commit()
                except psycopg2.ProgrammingError as e:
                    logger.error(to_execute)
                    logger.error("psycopg2({0}): {1}".format(e.errno, e.strerror))
                    continue
                finally:
                    self.conn.commit()

                if not row_id:
                    logger.error('Unable to update %s' % to_update)

        self._generate_sid_on_table()
        
    def load_to_db(self, stats):
        self.create_idtable()
        client_id = self.get_clientID()
        logger.debug("Got client %s" % client_id)
        datalist = Utils.read_tstatlog(self.dbconfig['tstatfile'], self.dbconfig['harfile'], "\n", client_id)
        logger.debug("len(datalist) = %d" % len(datalist))
        if len(datalist) > 0:
            self.write_plugin_into_db(datalist, stats)

    def execute_query(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        res = cur.fetchall()
        return res

    def execute_update(self, query):
        cur = self.conn.cursor()
        cur.execute(query)
        self.conn.commit()
        
    def _select_max_sid(self):
        query = "select max(sid) from %s" % self.dbconfig['rawtable']
        res = self.execute_query(query)
        max_sid = 0
        if res[0] != (None,):
            max_sid = int(res[0][0])
        return max_sid

    def _generate_sid_on_table(self):
        max_sid = self._select_max_sid()
        query = '''select distinct on (probe_id, session_start) probe_id, session_start from %s where sid is NULL
        order by session_start''' % self.dbconfig['rawtable']
        res = self.execute_query(query)
        logger.debug('Found %d sessions to insert', len(res))
        for i in range(len(res)):
            clientid = res[i][0]
            session_start = res[i][1]
            max_sid += 1
            query = '''update %s set sid = %d where session_start = \'%s\' and probe_id = \'%s\'''' \
                    % (self.dbconfig['rawtable'], max_sid, session_start, clientid)
            self.execute_update(query)
        return max_sid

    def quit_db(self):
        self.conn.close()

    def get_inserted_sid_addresses(self):
        result = {}
        q = '''select distinct a.sid, a.ip, b.session_url
        from %s a, %s b
        where a.sid = b.sid and not b.is_sent;''' \
            % (self.dbconfig['aggregatedetails'], self.dbconfig['aggregatesummary'])
        res = self.execute_query(q)
        for tup in res:
            sid = str(tup[0])
            if sid not in result.keys():
                result[sid] = {'url': tup[2], 'address': [tup[1]]}
            else:
                if tup[2] != result[sid]['url']:
                    logger.error("Misleading url in fetched data.")
                    logger.error("{0} -> {1}".format(res, tup))

                tmp_addrs = result[sid]['address']
                tmp_addrs.append(tup[1])
                result[sid]['address'] = list(set(tmp_addrs))

        return result

        #q = '''select distinct on (sid, session_url, remote_ip) sid, session_url, remote_ip
        #FROM %s where sid not in (select distinct sid from active)''' % self.dbconfig['rawtable']
        #res = self.execute_query(q)
        #for tup in res:
        #    cur_sid = tup[0]
        #    cur_url = tup[1]
        #    cur_addr = tup[2]
        #    if cur_addr == '0.0.0.0':
        #        continue
        #    if cur_sid in result.keys():
        #        if result[cur_sid]['url'] == cur_url:
        #            result[cur_sid]['address'].append(cur_addr)
        #        else:
        #            result[cur_sid]['url'] = cur_url
        #            result[cur_sid]['address'].append(cur_addr)
        #    else:
        #        result[cur_sid] = {'url': cur_url, 'address': [cur_addr]}
        #print 'result _get_inserted_sid_addresses', result
        #return result

    def insert_active_measurement(self, ip_dest, tot_active_measurement):
        #data['ping'] = json obj
        #data['trace'] = json obj
        cur = self.conn.cursor()
        for sid, data in tot_active_measurement.iteritems():
            for dic in data:
                url = dic['url']
                ip = dic['ip']
                ping = dic['ping']
                trace = dic['trace']
                query = '''INSERT into %s (ip_dest, sid, session_url, remote_ip, ping, trace, sent ) values
                ('%s', %d, '%s', '%s', '%s','%s', %r) ''' % (self.dbconfig['activetable'],
                                                         ip_dest, int(sid), url, ip, ping, trace, False)  #TODO remove false on table
                cur.execute(query)
            logger.info('inserted active measurements for sid %s: ' % sid)
        self.conn.commit()
    
    def get_table_names(self):
        return {'raw': self.dbconfig['rawtable'], 'active': self.dbconfig['activetable']}

    def force_update_full_load_time(self, sid):
        q = '''select session_start, end_time from %s where sid = %d''' % (self.dbconfig['rawtable'], sid)
        res = self.execute_query(q)
        session_start = list(set([x[0] for x in res]))[0]
        end_time = max(list(set([x[1] for x in res])))
        forced_load_time = int((end_time - session_start).total_seconds() * 1000)
        update = '''update %s set full_load_time = %d where sid = %d''' \
                 % (self.dbconfig['rawtable'], forced_load_time, sid)
        self.execute_update(update)
        return forced_load_time
        
    def check_for_zero_full_load_time(self):
        q = '''select sid from %s where full_load_time = 0''' % self.dbconfig['rawtable']
        res = self.execute_query(q)
        sids = [int(x[0]) for x in res]
        if len(sids) > 0:
            for s in sids:
                self.force_update_full_load_time(s)
                res.append(s)
        return res

    def pre_process_raw_table(self):
        # TODO: page_dim as sum of netw_bytes in summary
        logger.info('Pre-processing data from raw table...')
        # eliminate redirection (e.g., http://www.google.fr/?gfe_rd=cr&ei=W8c_VLu9OcjQ8geqsIGQDA)
        q = '''SELECT DISTINCT sid, full_load_time FROM %s GROUP BY sid, full_load_time HAVING COUNT(sid) > 1
        and sid not in (select distinct sid from %s)''' % \
            (self.dbconfig['rawtable'], self.dbconfig['aggregatesummary'])
        res = self.execute_query(q)
        if len(res) == 0:
            logger.warning('pre_process: no sids found')
            return

        d = dict(res)
        logger.debug("{0} session(s) to preprocess: sids {1} ".format(len(d.keys()), d.keys()))
        dic = {}
        for sid in d.keys():
            q = '''select remote_ip, session_url, session_start, cpu_percent, mem_percent from %s
            where sid = %d and session_url = uri''' % (self.dbconfig['rawtable'], sid)

            res = self.execute_query(q)
            if len(res) > 1:
                logger.warning("Multiple tuples for sid {0}: {1}".format(sid, res))

            dic[str(sid)] = {'server_ip': res[0][0], 'full_load_time': d[sid],
                             'session_start': res[0][2], 'session_url': res[0][1],
                             'cpu_percent': res[0][3], 'mem_percent': res[0][4],
                             'browser': []}

            q = '''select distinct on (remote_ip) remote_ip, count(*) as cnt, sum(app_rtt) as s_app,
            sum(rcv_time) as s_rcv, sum(body_bytes) as s_body, sum(syn_time) as s_syn from %s where sid = %d
            group by remote_ip;''' % (self.dbconfig['rawtable'], sid)
            res = self.execute_query(q)

            page_dim = 0
            for tup in res:
                dic[str(sid)]['browser'].append({'ip': tup[0], 'nr_obj': int(tup[1]), 'sum_http': int(tup[2]),
                                                 'sum_rcv_time': int(tup[3]), 'netw_bytes': int(tup[4]),
                                                 'sum_syn': int(tup[5])})
                page_dim += int(tup[4])

            dic[str(sid)].update({'page_dim': page_dim})

            for el in dic[str(sid)]['browser']:
                ip = el['ip']
                q = '''select uri from %s where remote_ip = \'%s\'''' % (self.dbconfig['rawtable'], ip)
                res = self.execute_query(q)
                el.update({'base_url': '/'.join(res[0][0].split('/')[:3])})
                #if len(res) > 1:
                #    el.update({'base_url': os.path.commonprefix([x[0] for x in res])})
                #else:
                #    el.update({'base_url': '/'.join(res[0][0].split('/')[:3])})

        return self.insert_to_aggregate(dic)

    def insert_to_aggregate(self, pre_processed):
        logger.debug("received at insert_to_aggregate: {0}".format(pre_processed))
        table_name_summary = self.dbconfig['aggregatesummary']
        table_name_details = self.dbconfig['aggregatedetails']
        stub = 'INSERT INTO ' + table_name_summary + ' (%s) values (%s)'
        stub2 = 'INSERT INTO ' + table_name_details + ' (%s) values (%s)'
        for sid, obj in pre_processed.iteritems():
            url = DBClient._unicode_to_ascii(obj['session_url'])
            start = obj['session_start']
            flt = obj['full_load_time']
            page_dim = obj['page_dim']
            ip = obj['server_ip']
            cpu_percent = obj['cpu_percent']
            mem_percent = obj['mem_percent']
            q = stub % ('sid, session_url, session_start, full_load_time, page_dim, server_ip, cpu_percent, '
                        'mem_percent, is_sent', "%d, '%s', '%s', %d, %d, '%s', %d, %d, %r" %
                        (int(sid), url, start, flt, page_dim, ip, cpu_percent, mem_percent, False))
            cursor = self.conn.cursor()
            try:
                cursor.execute(q)
                #res = cursor.fetchone()
                #reference = int(res[0])
                self.conn.commit()
            except psycopg2.IntegrityError as e:
                logger.error("Integrity Error in insert to aggregate {0}".format(e))
                continue
            except psycopg2.InternalError as i:
                logger.error("Internal Error in insert to aggregate {0}".format(e))
                continue

            #if reference:
            for dic in (obj['browser']):
                s = 'sid, base_url, ip, netw_bytes, nr_obj, sum_syn, sum_http, sum_rcv_time'
                v = '%d, \'%s\', \'%s\', %d, %d, %d, %d, %d' % (int(sid), dic['base_url'], dic['ip'],
                                                                dic['netw_bytes'], dic['nr_obj'],
                                                                dic['sum_syn'], dic['sum_http'],
                                                                dic['sum_rcv_time'])
                q = stub2 % (s, v)
                cursor.execute(q)
                self.conn.commit()

        logger.info('Aggregate tables populated.')
        return True


if __name__ == '__main__':
    from Configuration import Configuration
    c = Configuration('probe.conf')
    d = DBClient(c)
    print d.get_inserted_sid_addresses()