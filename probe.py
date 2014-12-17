#!/usr/bin/python
import sys
import os
import shutil
import logging
import logging.config
from probe.Configuration import Configuration
from probe.PJSLauncher import PJSLauncher
from probe.DBClient import DBClient
from probe.ActiveMeasurement import Monitor
from probe.JSONClient import JSONClient
#from probe.TstatLiveCapture import TstatLiveCapture
#import time
import subprocess
import threading
import socket
import tarfile

logging.config.fileConfig('logging.conf')


class TstatDaemonThread(threading.Thread):

    def __init__(self, config, flag):
        self.flag = flag
        if self.flag == 'start':
            self.script = config.get_tstat_configuration()['start']
            self.tstatpath = os.path.join(config.get_tstat_configuration()['dir'], 'tstat/tstat')
            self.interface = config.get_tstat_configuration()['netinterface']
            self.netfile = config.get_tstat_configuration()['netfile']
            self.outdir = config.get_tstat_configuration()['tstatout']
            self.is_daemon = True
        else:
            self.script = config.get_tstat_configuration()['stop']
            self.is_daemon = False

        thread = threading.Thread(target=self.run, args=())
        thread.daemon = self.is_daemon
        logger.debug("TstatDaemonThread running [%s] is_daemon = %s..." % (os.path.basename(self.script), str(self.is_daemon)))
        thread.start()

    def run(self):
        if self.flag == 'start':
            cmd = "%s %s %s %s %s" % (self.script, self.tstatpath, self.interface, self.netfile, self.outdir)
            p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False).wait()
        else:
            p = subprocess.Popen(self.script, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False).wait()


def clean_files(bdir, tstat_out, harf, run_nr, sessionurl, error=False):
    if error:
        fn = bdir + '/' + tstat_out.split('/')[-1] + '.run%d_%s.error' % (run_nr, sessionurl)
        os.remove(harf)
    else:
        fn = bdir + '/' + tstat_out.split('/')[-1] + '.run%d_%s' % (run_nr, sessionurl)
        har = bdir + '/' + harf.split('/')[-1] + '.run%d_%s' % (run_nr, sessionurl)
        os.rename(harf, har)
    shutil.copyfile(tstat_out, fn)  # Quick and dirty not to delete Tstat log
    open(tstat_out, 'w').close()
    return fn

if __name__ == '__main__':
    if len(sys.argv) < 4:
        exit("Usage: %s %s %s %s" % (sys.argv[0], 'nr_runs', 'conf_file', 'backup folder'))
    nun_runs = int(sys.argv[1])
    conf_file = sys.argv[2]
    backupdir = sys.argv[3]
    logger = logging.getLogger('probe')
    config = Configuration(conf_file)        

    tstat_out_file = config.get_database_configuration()['tstatfile']
    harfile = config.get_database_configuration()['harfile']
    launcher = PJSLauncher(config)    
    
    logger.debug('Backup dir set at: %s' % backupdir)
    dbcli = DBClient(config, create=True)
    logger.debug('Starting nr_runs (%d)' % nun_runs)
    pjs_config = config.get_phantomjs_configuration()
    t = TstatDaemonThread(config, 'start')
    for i in range(nun_runs):
        for url_in_file in open(pjs_config['urlfile']):
            url = url_in_file.strip()
            #ip_dest = socket.gethostbyname(url)
            #logger.debug('Resolved %s to [%s]' % (url, ip_dest))
            try:
                stats = launcher.browse_url(url)
            except AttributeError:
                logger.error("Problems in browser thread. Aborting session...")
                browser_error = True
                break
            #logger.debug('Received stats: %s' % str(stats))
            if stats is None:
                logger.warning('Problem in session %d [%s - %s].. skipping' % (i, url, ip_dest))
                # clean temp files
                clean_files(backupdir, tstat_out_file, harfile, i, url, True)
                continue
            if not os.path.exists(tstat_out_file):
                logger.error('tstat outfile missing. Check your network configuration.')
                exit("tstat outfile missing. Check your network configuration.")

            dbcli.load_to_db(stats)
            #logger.debug('Loaded stats run n.%d for %s' % (i, url))
            logger.info('Ended browsing run n.%d for %s' % (i, url))
            dbcli.pre_process_raw_table()

            new_fn = clean_files(backupdir, tstat_out_file, harfile, i, url)
            #new_fn = backupdir + '/' + tstat_out_file.split('/')[-1] + '.run%d_%s' % (i, url)
            #shutil.copyfile(tstat_out_file, new_fn)  # Quick and dirty not to delete Tstat log
            #open(tstat_out_file, 'w').close()
            #new_har = backupdir + '/' + harfile.split('/')[-1] + '.run%d_%s' % (i, url)
            #os.rename(harfile, new_har)
            logger.debug('Saved plugin file for run n.%d: %s' % (i, new_fn))
            monitor = Monitor(config)
            #monitor.do_measure(ip_dest)
            monitor.run_active_measurement()
            logger.debug('Ended Active probing for run n.%d to url %s' % (i, url))
            for tracefile in [f for f in os.listdir('.') if f.endswith('.traceroute')]:
                os.remove(tracefile)
        else:
            print ("run {0} done.".format(i))
            continue
        # here we go if break in inner loop
        logger.error("Forcing tstat to stop.")
        s = TstatDaemonThread(config, 'stop')  # TODO check if tstat really quit
        exit(1)


    s = TstatDaemonThread(config, 'stop')  # TODO check if tstat really quit
    jc = JSONClient(config)
    measurements = jc.prepare_data()
    json_path_fname = jc.save_json_file(measurements)
    jc.send_json_to_srv(measurements)
    logger.info('Probing complete. Packing Backups...')

    fname = os.path.basename(json_path_fname)
    dest_file = os.path.join(backupdir, fname)
    os.rename(json_path_fname, dest_file)

    for root, _, files in os.walk(backupdir):
        if len(files) > 0:
            tar = tarfile.open("%s.tar.gz" % backupdir, "w:gz")
            tar.add(backupdir)
            tar.close()
            logger.info('Tar.gz backup file created.')
    shutil.rmtree(backupdir)
    logger.info('Done. Exiting.')
    exit(0)
