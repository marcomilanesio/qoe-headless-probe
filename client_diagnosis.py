#!/usr/bin/python

import sys
import json
from probe.JSONClient import JSONClient
from probe.Configuration import Configuration



def ask_diagnosis(conf_file, url):
    configuration = Configuration(conf_file)
    jc = JSONClient(configuration)
    res = jc.send_request_for_diagnosis(url, 6) # time range...TODO
    for session in json.loads(res['return']):
        for k, v in session.iteritems():
            print 'Session %s result: %s' % (k, v)
    

if __name__ == '__main__':
    if len(sys.argv) != 3:
        exit("Usage: %s <conf_file> <url>" % sys.argv[0])
    
    config_file = sys.argv[1]
    url_to_check = sys.argv[2]
    ask_diagnosis(config_file, url_to_check)
    
