#!/usr/bin/env python

import os
import sys
import time
import tempfile
import urllib2

TIME_BETWEEN_STARTS = 0.5

m_grails_tempfile = None


def nmea_simulator_proclist():
    #(friendly name,
    # base directory [relative to current virualenv],
    # executable that you'd type
    #)
    return [
        ("grails",    
         "ooici-pres-ifreecarve",
         "sh -c 'fab deployLocal < %s'" % grails_tempfile()),

        ("nmea-sim",
         "ioncore-python",
         "bin/ipython ion/agents/instrumentagents/simulators/sim_NMEA0183_preplanned.py"),

        ("twistd", 
         "ioncore-python",
         "bin/twistd -n cc -a sysname=ijk5 -h localhost res/deploy/r1deploy.rel"),

        ]

        
def make_grails_tempfile():
    (handle, path) = tempfile.mkstemp('txt', 'nmeaSim-', None, True)
    os.write(handle,"ooici-pres-0.1\r\n")
    os.write(handle,"localhost\r\n")
    os.write(handle,"ijk5\r\n")
    os.write(handle,"5672\r\n")
    os.write(handle,"ems\r\n")
    os.write(handle,"ems\r\n")
    os.write(handle,"magnet.topic\r\n")
    os.write(handle,"http://192.168.56.3:9998\r\n")
    os.write(handle,"force\r\n")
    os.close(handle)
    return path

def grails_tempfile():
    global m_grails_tempfile
    if None == m_grails_tempfile:
        m_grails_tempfile = make_grails_tempfile()
    
    return m_grails_tempfile


if __name__ == "__main__":

    debug = 1 < len(sys.argv) and "debug" == sys.argv[1]

    for p in nmea_simulator_proclist():
                
        print "Starting", p[0].ljust(15), "in new screen...",

        if debug: print ""
        wd = os.path.join(os.environ["VIRTUAL_ENV"], p[1])
        if debug: 
            print "\tcd", wd
        else:
            os.chdir(wd)

        cm = "screen -dmS %s %s" % (p[0], p[2])
        if debug: 
            print "\t" + cm
        else:
            os.system(cm)

        print "OK"
        time.sleep(TIME_BETWEEN_STARTS)

    if debug:
        print grails_tempfile()
    else:
        #wait for grails to come up
        sys.stdout.write("\nWaiting for grails to come up on localhost")
        keepgoing = True
        while keepgoing:
            try:
                urllib2.urlopen("http://localhost:8080")
                print "OK"
                keepgoing = False
            except urllib2.URLError:
                sys.stdout.write(".")
                sys.stdout.flush()
                time.sleep(0.5)
            except Exception, e:
                print "\nERROR:", e.value
                keepgoing = False
                
        os.unlink(grails_tempfile())

    exit(0)


