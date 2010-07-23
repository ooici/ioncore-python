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


class HostStatusTest(IonTestCase):
    """
    Testing client classes of host_status
    """
    HOST_STATUS_DAEMON = 'ion/services/coi/hostsensor/host_status_daemon.py'
    
    @defer.inlineCallbacks
    def setUp(self):
        logging.debug('Starting Host Status XMLRPC Server')
        p = subprocess.Popen(
                     [sys.executable, self.HOST_STATUS_DAEMON,'start'], 
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE
                     )
        # wait 5 seconds for the daemon to shutdown
        retries = 5
        while not p.returncode == None and retries > 0:
            logging.debug('Waiting for server to start')
            yield pu.asleep(1)
            retries -= 1
        logging.debug('Server started with returncode %s'%str(p.returncode))
        yield self._start_container()


    @defer.inlineCallbacks
    def tearDown(self):
        p = subprocess.Popen(
                     [sys.executable, self.HOST_STATUS_DAEMON,'stop'], 
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE
                     )
        # wait 5 seconds for the daemon to shutdown
        retries = 5
        while not p.returncode == None and retries > 0:
            yield pu.asleep(1)
            retries -= 1
        yield self._stop_container()



    @defer.inlineCallbacks
    def test_BasicService(self):
        # Test with seperate store backends
        services = [
            {'name':'hoststatus1','module':'ion.services.coi.host_status','class':'HostStatusService','spawnargs':{}}
        ]
        sup = yield self._spawn_processes(services)
        yield pu.asleep(10)

