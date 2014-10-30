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
import subprocess
import psutil
import time
from probe.Configuration import Configuration
import logging
import threading
import os
import re

logger = logging.getLogger('PJSLauncher')


class BrowserThread(threading.Thread):
    def __init__(self, cmd, outf, errf):
        self.cmd = cmd
        self.process = None
        self.outfile = outf
        self.errfile = errf
        self.mem = -1
        self.cpu = -1
        self.flag = False

    def run(self, timeout):
        def target():
            o = open(self.outfile, 'a')
            e = open(self.errfile, 'a')
            logger.debug('Browsing Thread started')
            FNULL = open(os.devnull, 'w')
            self.process = subprocess.Popen(self.cmd, stdout=FNULL, stderr=e, shell=True)
            memtable = []
            cputable = []
            while self.process.poll() is None:
                arr = psutil.cpu_percent(interval=0.1, percpu=True)
                cputable.append(sum(arr) / float(len(arr)))
                memtable.append(psutil.virtual_memory().percent)
                time.sleep(1)
            #if self.process.poll() == 0:
            self.mem = float(sum(memtable) / len(memtable))
            self.cpu = float(sum(cputable) / len(cputable))
            #print 'mem = %.2f, cpu = %.2f' % (self.mem, self.cpu)
            #else:
                #logger.error('Browsing thread mem and cpu not set')            
            logger.debug('Browsing Thread finished')
            o.close()
            e.close()

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            logger.debug('Timeout expired: terminating process')
            try:
                self.process.terminate()
            except AttributeError as e:
                logger.error('Error in browser thread {0} {1}'.format(e.errno, e.strerror))
                return self.flag, self.mem, self.cpu, True
            finally:
                self.flag = True
                thread.join()
        
        return self.flag, self.mem, self.cpu, False
                

class PJSLauncher():
    def __init__(self, config):
        self.config = config
        self.pjs_config = self.config.get_phantomjs_configuration()
        self.osstats = {}
        logger.debug('Loaded configuration')

    def browse_urls(self):	
        for url in open(self.pjs_config['urlfile']):	    
            if not re.match('http://', url):
                use = 'http://' + url.strip()
            else:
                use = url.strip()
            if use[-1] != '\/':
                use += '/'
            self.osstats[use] = self.browse_url(use)

        return self.osstats

    def browse_url(self, urlx):
        #out = open(self.config['logfile'], 'a')
        if not re.match('http://', urlx):
            url = 'http://' + urlx.strip()
        else:
            url = urlx.strip()
        if url[-1] != '\/':
            url += '/'
        logger.info('Browsing %s', url)
        res = {'mem': 0.0, 'cpu': 0.0}
        cmdstr = "%s/bin/phantomjs %s %s" % (self.pjs_config['dir'], self.pjs_config['script'], url)
        cmd = BrowserThread(cmdstr, self.pjs_config['thread_outfile'], self.pjs_config['thread_errfile'])
        t = int(self.pjs_config['thread_timeout'])
        flag, mem, cpu, error = cmd.run(timeout=t)
        if not error:
            logger.debug('browserthread {0} {1} {2}'.format(flag, mem, cpu))
        else:
            logger.error('Error in browsing thread reported.')
        if not flag:
            res['mem'] = mem
            res['cpu'] = cpu
            logger.info('%s: mem = %.2f, cpu = %.2f' % (url, res['mem'], res['cpu']))
        else:
            logger.warning('Problems in browsing thread. Need to restart browser...')
            time.sleep(5)
        #out.close()
        self.osstats[url] = res
        return self.osstats

    '''
    def dump_data_on_error(self):
        import datetime
        import time
        now = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        dumpfile = './dumped.plugin.file.%s' % now
        plugin_file = self.config.get_database_configuration()['pluginoutfile']
        #os.rename(plugin_file, dumpfile)
        logger.info('Dumped plugin out file to: %s' % dumpfile)
        logger.info('Sleeping 5 seconds...')
        time.sleep(5)
        
        return None
    '''
        
if __name__ == '__main__':
    f = PJSLauncher()
    print f.browse_urls()
