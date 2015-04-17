#!/usr/bin/python3
import sys
import os
import shutil
import subprocess
import threading
import tarfile
import logging
import logging.config
import urllib.request
import json
import time
import datetime

from optparse import OptionParser
from probe.Configuration import Configuration
from probe.PJSLauncher import PJSLauncher
from probe.ActiveMeasurement import Monitor
from probe.JSONClient import JSONClient

from db.dbclient import DBClient
from diagnosis.localdiagnosis import LocalDiagnosisManager


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


def get_location():
    loc_request = urllib.request.Request('http://ipinfo.io')
    loc_request.add_header('User-Agent', 'curl/7.30.0')
    response = urllib.request.urlopen(loc_request)
    return json.loads(response.read().decode('utf-8'))


def start_flume_process(config):
    ex = "{0}{1}".format(config.get_flume_configuration()['flumedir'], "/bin/flume-ng")
    confdir = config.get_flume_configuration()['confdir']
    conffile = config.get_flume_configuration()['conffile']
    agentname = config.get_flume_configuration()['agentname']
    #flumelogging = "-Dflume.root.logger=INFO,console"
    cmd = "{0} agent -c {1} -f {2} -n {3}".format(ex, confdir, conffile, agentname) #, flumelogging)
    proc = subprocess.Popen(cmd.split())
    return proc


def stop_flume_process(proc):
    proc.kill()


def check_fs(config, backup_dir):
    if not os.path.isdir(config.get_flume_configuration()['outdir']):
        os.makedirs(config.get_flume_configuration()['outdir'])
    if not os.path.isdir(backup_dir):
        os.makedirs(backup_dir)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-c", "--conf", dest="conf_file", help="specify a configuration file", metavar="FILE", default='./probe.conf')
    parser.add_option("-n", "--runs", dest="num_runs", help="specify the number of runs", metavar="INT", default=1)
    parser.add_option("-d", "--backup", dest="backup_dir", help="specify the directory for backups", metavar="DIR", default='./session_bkp')
    (options, args) = parser.parse_args()
    #print(options.num_runs)
    if len(args) != 3:
        print("Use -h for complete list of options")
        print("Launching with: {0}".format(options))

    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d-%H%M%S')
    backup_dir = os.path.join(options.backup_dir, st)

    logger = logging.getLogger('probe')
    config = Configuration(options.conf_file)
    logger.debug("Launching the probe...")
    check_fs(config, backup_dir)

    tstat_out_file = config.get_database_configuration()['tstatfile']
    harfile = config.get_database_configuration()['harfile']
    launcher = PJSLauncher(config)    
    
    logger.debug('Backup dir set at: %s' % backup_dir)
    loc_info = get_location()
    if not loc_info:
        logger.warning("No info on location retrieved.")
    dbcli = DBClient(config, loc_info, create=True)
    flumeprocess = start_flume_process(config)
    if flumeprocess:
        logger.debug("Flume agent started")
    else:
        logger.error("Flume not started.. no data will be sent to repository")

    logger.debug('Starting nr_runs (%d)' % options.num_runs)
    pjs_config = config.get_phantomjs_configuration()
    t = TstatDaemonThread(config, 'start')
    for i in range(options.num_runs):
        for url_in_file in open(pjs_config['urlfile']):
            url = url_in_file.strip()
            try:
                stats = launcher.browse_url(url)
            except AttributeError:
                logger.error("Problems in browser thread. Aborting session...")
                browser_error = True
                break
            if stats is None:
                logger.warning('Problem in session %d [%s].. skipping' % (i, url))
                # clean temp files
                clean_files(backup_dir, tstat_out_file, harfile, i, url, True)
                continue
            if not os.path.exists(tstat_out_file):
                logger.error('tstat outfile missing. Check your network configuration.')
                exit("tstat outfile missing. Check your network configuration.")

            dbcli.load_to_db(stats)
            logger.info('Ended browsing run n.%d for %s' % (i, url))
            passive = dbcli.pre_process_raw_table()
            new_fn = clean_files(backup_dir, tstat_out_file, harfile, i, url)

            logger.debug('Saved plugin file for run n.%d: %s' % (i, new_fn))
            monitor = Monitor(config, dbcli)
            active = monitor.run_active_measurement()
            logger.debug('Ended Active probing for run n.%d to url %s' % (i, url))
            for tracefile in [f for f in os.listdir('.') if f.endswith('.traceroute')]:
                os.remove(tracefile)
            l = LocalDiagnosisManager(dbcli, url)
            diagnosis = l.run_diagnosis(passive, active)
        else:
            logger.info("run {0} done.".format(i))
            print("run {0} done.".format(i))
            continue
        # here we go if break in inner loop
        logger.error("Forcing tstat to stop.")
        s = TstatDaemonThread(config, 'stop')  # TODO check if tstat really quit
        exit(1)

    s = TstatDaemonThread(config, 'stop')  # TODO check if tstat really quit
    jc = JSONClient(config, dbcli)
    measurements = jc.prepare_data()
    json_path_fname = jc.save_json_file(measurements)
    jc.send_json_to_srv(measurements)
    logger.info('Probing complete. Packing Backups...')

    stop_flume_process(flumeprocess)

    fname = os.path.basename(json_path_fname)
    dest_file = os.path.join(backup_dir, fname)
   # os.rename(json_path_fname, dest_file)
    shutil.copyfile(json_path_fname, dest_file)	

    for root, _, files in os.walk(backup_dir):
        if len(files) > 0:
            tar = tarfile.open("%s.tar.gz" % backup_dir, "w:gz")
            tar.add(backup_dir)
            tar.close()
            logger.info('tar.gz backup file created.')
    shutil.rmtree(backup_dir)
    logger.info('Done. Exiting.')
    exit(0)
