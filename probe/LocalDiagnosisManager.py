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

logger = logging.getLogger('LocalDiagnosisManager')


class LocalDiagnosisManager():
    def __init__(self, db, url):
        self.dbconn = db
        self.url = url
        self.localdata = self.get_local_diagnosis_data()
        logger.debug("LocalDiagnosisManager started for {0}".format(self.url))

    def get_local_diagnosis_data(self):
        q = '''select flt, http, tcp, dim, t1, d1, d2, dh
        from {0} where url like '%{1}%' '''.format(self.dbconn.get_table_names()['localdiagnosistable'], self.url)
        try:
            res = self.dbconn.execute_query(q)
            data = res[0]
            return {'full_load_time_th': data[0], 'http_th': data[1], 'tcp_th': data[2], 'dim_th': data[3],
                    't1': data[4], 'd1': data[5], 'd2': data[6], 'dh': data[7]}
        except:
            return None

    def insert_first_time(self, flt, http, tcp, dim):
        flt += 1000
        http += 500
        tcp += 500
        dim += 300000
        t1 = 0.1
        d1 = 3
        d2 = 3
        dh = 500
        template = '''insert into {0} (url, flt, http, tcp, dim, t1, d1, d2, dh) values '''\
            .format(self.dbconn.get_table_names()['localdiagnosistable'])
        default = "('{0}', {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})".format(self.url, flt, http, tcp, dim,
                                                                           t1, d1, d2, dh)
        q = "{0}{1}".format(template, default)
        self.dbconn.execute_update(q)
        return {'full_load_time_th': flt, 'http_th': http, 'tcp_th': tcp, 'dim_th': dim,
                't1': t1, 'd1': d1, 'd2': d2, 'dh': dh}

    def do_local_diagnosis(self):
        res = {}
        q = '''select sid, session_start, server_ip, full_load_time, page_dim, cpu_percent, mem_percent
        from {0} where sid in (select max(sid) from {0} where session_url like '%{1}%')
        '''.format(self.dbconn.get_table_names()['summarytable'], self.url)
        res = self.dbconn.execute_query(q)
        assert len(res) == 1
        session = res[0]
        sid, starttime, servip, flt, dim, cpu, mem = session

        q = '''select base_url, ip, netw_bytes, sum_syn, sum_http, sum_rcv_time
        from {0} where sid = {1};'''.format(self.dbconn.get_table_names()['detailstable'], sid)
        res = self.dbconn.execute_query(q)
        details = {}
        for tup in res:
            baseurl, ip, netw, syn, http, rcv = tup
            details[ip] = {'baseurl': baseurl, 'netw': netw, 'syn': syn, 'http': http, 'rcv': rcv}

        q = '''select remote_ip, ping, trace from {0} where sid = {1}'''\
            .format(self.dbconn.get_table_names()['activetable'], sid)
        res = self.dbconn.execute_query(q)
        active = {}
        for tup in res:
            ip, ping, trace = tup
            active[ip] = {'ping': ping}
            if trace is not None:
                active[ip].update({'trace': trace})

        return self.run_diagnosis(session, details, active)

    def run_diagnosis(self, session_dic, details_dic, active_dic):
        sid, starttime, servip, flt, dim, cpu, mem = session_dic
        http_times = [dic['http'] for ip, dic in details_dic.iteritems()]
        tcp_times = [dic['syn'] for ip, dic in details_dic.iteritems()]

        if self.localdata is None:  # first time we hit the url
            self.insert_first_time(flt, sum(http_times), sum(tcp_times), dim)

        self.localdata = self.get_local_diagnosis_data()

        cpu_th = mem_th = 50

        if flt < self.localdata['full_load_time_th']:
            logger.info("{0} = No problem".format(self.url))
            return

        if cpu > cpu_th or mem > mem_th:
            logger.info("{0} = Local probe overloaded".format(self.url))
            return
        if sum(http_times) < self.localdata['http_th']:
            if dim > self.localdata['dim_th']:
                logger.info("{0} = Page too big".format(self.url))
            elif sum(tcp_times) > self.localdata['tcp_th']:
                logger.info("{0} = Web server too far".format(self.url))
            else:
                logger.info("{0} = Network generic".format(self.url))
            return
        else:
            logger.info("{0} = Network generic".format(self.url))

    def _execute_obj_start_end_query(self, sid, full_load_time=True):
        q = '''select session_start, obj_start, obj_end, httpid, host,
            extract(minute from obj_start-session_start)*60*1000+extract(millisecond from obj_start-session_start) as relative_start, 
            extract(minute from obj_end-session_start)*60*1000+extract(millisecond from obj_end-session_start) as relative_end 
            from 
            (SELECT session_start, 
            case when dns_start>'1970-01-01 12:00:00' and dns_start<syn_start and dns_start<get_sent_ts then dns_start
            when syn_start>'1970-01-01 12:00:00' and syn_start<get_sent_ts then syn_start
            when get_sent_ts>'1970-01-01 12:00:00' then get_sent_ts else request_ts end as obj_start,
            case when end_time>'1970-01-01 12:00:00' then end_time
            when first_bytes_rcv>'1970-01-01 12:00:00' then first_bytes_rcv else request_ts end as obj_end,
            httpid, host from %s where sid=%d and cache = 0
            ''' % (self.dbconn.get_table_names()['raw'], sid)
        
        if full_load_time:
            q += ' and full_load_time > -1)t'
        else:
            q += ')t'
        
        res = self.dbconn.execute_query(q)
        return res
    
    def _get_client_idle_time(self, sid):
        found_zero = self.dbconn.check_for_zero_full_load_time()
        if len(found_zero) > 0:
            logger.warning('Found full_load_time = 0 in sessions %s. Updated.' % str(found_zero))
        res = self._execute_obj_start_end_query(sid)
        if len(res) == 0:
            logger.warning('sid %d, probe %d : full_load_time = -1' % (sid, self.clientid))
            res = self._execute_obj_start_end_query(sid, full_load_time=False)
            logger.warning('sid %d, probe %d : with no check on full_load_time, found %d objects' % (sid, self.clientid, len(res)))
            new_full_load_time = self.dbconn.force_update_full_load_time(sid)
            logger.warning('sid %d, probe %d : forced full_load_time = %d ' % (sid, self.clientid, new_full_load_time))
        session_start = str(res[0][0])  # convert datetime to string for being json-serializable
        rel_starts = [r[5] for r in res]
        rel_ends = [r[6] for r in res]
        
        idle_time = 0.0
        end = rel_ends[0]
        for i in range(1, len(rel_starts)):
            if rel_starts[i] > end:
                idle_time += rel_starts[i] - end
            end = rel_ends[i]
        logger.debug("_get_client_idle_time: {0} {1}".format(session_start, idle_time))
        return session_start, idle_time  #msec
    
    def _get_http_response_time(self, sid):
        q = '''select app_rtt from %s where sid = %d and full_load_time > -1'''\
            % (self.dbconn.get_table_names()['raw'], sid)
        res = self.dbconn.execute_query(q)
        logger.debug('{0}'.format(res))
        app_rtts = [r[0] for r in res]
        http_res_time = -1
        if len(app_rtts) == 0:
            logger.warning('_get_http_response_time got 0 results')
        else:
            #print Utils.computeQuantile(app_rtts, 0.5)
            http_res_time = sum(app_rtts)/float(len(app_rtts))
            logger.debug("_get_http_response_time = {0}".format(http_res_time))
        return http_res_time

    def _get_page_downloading_time(self, sid):
        q = '''select distinct full_load_time from %s where sid = %d and full_load_time > -1 group by full_load_time'''\
            % (self.dbconn.get_table_names()['raw'], sid)
        res = self.dbconn.execute_query(q)
        page_down = -1
        if len(res) == 0:
            logger.warning('_get_page_downloading_time got 0 results')
        else:
            page_down = float(res[0][0]) #msec
            logger.debug("_get_page_downloading_time = {0}".format(page_down))
        return page_down

    def _get_dns_response_time(self, sid):
        q = '''select remote_ip, dns_time from %s where sid = %d and dns_time > 0 and full_load_time > -1'''\
            % (self.dbconn.get_table_names()['raw'], sid)
        res = self.dbconn.execute_query(q)
        #resolved_ips = [r[0] for r in res]
        dns_times = [float(r[1]) for r in sorted(res, key=lambda time: time[1])]
        return sum(dns_times)  #msec

    def _get_tcp_response_time(self, sid):
        q = 'select syn_time from %s where sid = %d and full_load_time > -1' % (self.dbconn.get_table_names()['raw'], sid)
        res = self.dbconn.execute_query(q)
        tcp_times = [r[0] for r in res]
        tcp_resp = -1
        if len(tcp_times) == 0:
            logger.warning('_get_tcp_response_time got 0 results')
        else:
            tcp_resp = sum(tcp_times) / float(len(tcp_times))  #msec
            logger.debug("_get_tcp_response_time = {0}".format(tcp_resp))
        return tcp_resp

    def _get_page_dimension(self, sid):
        # header_bytes missing in phantomJS
        q = '''SELECT sum(header_bytes + body_bytes) as netw_bytes, count(*) as nr_netw_obj
            from %s where sid = %d and full_load_time > -1''' % (self.dbconn.get_table_names()['raw'], sid)
        res = self.dbconn.execute_query(q)
        assert len(res) == 1
        if res[0][0] is not None:
            tot_bytes = int(res[0][0])
            nr_obj = int(res[0][1])
        else:
            tot_bytes = 0
        logger.debug("_get_page_dimension = {0}".format(tot_bytes))
        return tot_bytes

    def _get_os_stats(self, sid):
        q = '''select distinct on(cpu_percent, mem_percent) cpu_percent, mem_percent from %s where sid = %d'''\
            % (self.dbconn.get_table_names()['raw'], sid)
        res = self.dbconn.execute_query(q)
        r = [-1, -1]
        if len(res) != 1:
            logger.error('multiple sessions with sid = %d' % sid)
            return r
        stats = res[0]
        try:
            r = [float(x) for x in stats]
        except TypeError:
            logger.error("Unable to find cpu and mem stats for sid %s " % sid)
        logger.debug("_get_os_stats = {0}".format(r))
        return r

    def _get_ip_dest(self, sid):
        q = '''select distinct ip_dest, count(*) as cnt from %s where sid = %d group by ip_dest order by cnt desc''' % \
            (self.dbconn.get_table_names()['active'], sid)
        res = self.dbconn.execute_query(q)
        if len(res) == 0:
            logger.error("No destination ip found for sid {0}".format(sid))
            ip_dest = -1
        if len(res) > 1:
            logger.warning("Found more destination ip for sid {0}: {1}".format(sid, res))
        ip_dest = res[0][0]
        return ip_dest