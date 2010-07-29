#!/usr/bin/env python

"""
@file ion/services/coi/test/test_host_status.py
@author Brian Fox
@brief test service for sending host_ tatus messages
"""

import logging, subprocess, sys
logging = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest
from twisted.internet.task import LoopingCall

from ion.services.coi.hostsensor.readers import HostReader
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.coi.host_status import HostStatusClient


class HostStatusTest(IonTestCase):
    """
    Testing client classes of host_status
    """
    
    HOST_STATUS_DAEMON = 'ion/services/coi/hostsensor/host_status_daemon.py'
    
    
    
    @defer.inlineCallbacks
    def _start_xmlrpc_daemon(self):
        """
        Starts a dependent XMLRPC server (daemon) on the local host.  The
        daemon serves SNMP and other host data.
        """    
        
        logging.debug('Starting Host Status XMLRPC Server')
        p = subprocess.Popen(
                     [sys.executable, self.HOST_STATUS_DAEMON,'start'], 
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE
                     )
        
        # wait 10 seconds for the daemon to power up
        retries = 20
        while p.poll() is None and retries > 0:
            logging.debug('Waiting for server to start.  Poll status: %s  Retries Left: %s'%(p.poll(),retries))
            yield pu.asleep(0.5)
            retries -= 1
        logging.debug('XMLRPC daemon started with return code %s'%str(p.returncode))
        self.assertEqual(p.returncode,0,"XMLRPC daemon started uncleanly.  Already running?")


    @defer.inlineCallbacks
    def _stop_xmlrpc_daemon(self):
        """
        Stops the XMLRPC server (daemon) on the local host.
        """    
        p = subprocess.Popen(
                     [sys.executable, self.HOST_STATUS_DAEMON,'stop'], 
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE
                     )
        # wait 10 seconds for the daemon to shutdown
        retries = 20
        while p.poll() is None and retries > 0:
            logging.debug('Waiting for server to stop.  Poll status: %s  Retries Left: %s'%(p.poll(),retries))
            yield pu.asleep(0.5)
            retries -= 1
        logging.debug('XMLRPC daemon stopped with return code %s'%str(p.returncode))
        self.assertEqual(p.returncode,0,"XMLRPC daemon stopped uncleanly.")


    @defer.inlineCallbacks
    def _start_service(self):
        messaging = {'registry':{'name_type':'fanout', 'args':{'scope':'system'}}}
        yield self._declare_messaging(messaging)

        # By default ion.services.coi.host_status loops indefinitely
        # at 60 second intervals.  The default behavior can be modified 
        # with spawnargs.
        # count = 1 : run a single iteration 
        # interval = 1 : meaningless for a single iteration

        services = [
            {
                'name':'hoststatus1',
                'module':'ion.services.coi.host_status',
                'class':'HostStatusService',
                'spawnargs':
                    {
                        'sys-name':'host_status',
                        'servicename':'host_status',
                        'count':1,
                        'interval':1
                    }
            }
        ]
        yield self._spawn_processes(services)


    @defer.inlineCallbacks
    def setUp(self):
        # self.client = HostStatusClient()
        yield self._start_xmlrpc_daemon()
        yield self._start_container()
        yield self._start_service()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_xmlrpc_daemon()
        yield self._stop_container()
        

    @defer.inlineCallbacks
    def test_BasicService(self):
        retries = 20
        while retries > 0:
            retries -= 1
            print 'Waiting...'
            yield pu.asleep(0.5)
