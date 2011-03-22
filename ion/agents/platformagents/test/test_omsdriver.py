#!/usr/bin/env python
"""
@file ion/agents/platformagents/test/test_omsdriver.py
@brief This module has test cases to test out the OMS driver.
@author Steve Foley
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.util.itv_decorator import itv
from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.test.iontest import IonTestCase
from ion.core.process.process import ProcessClient
import ion.util.procutils as pu

class TestOmsDriver(IonTestCase):
    """
    Tests from this class require a connection to the RSN OMS system. This may
    require use of a VPN client that has been properly configured to access the
    OMS software. Access involves the VPN user, password, address, and port
    to be correct, along with no other VPNs running on the designated machine.
    The RSN folks can help get that setup on the computers running these tests.
    
    This test case is also very much a basic prototype and tests should be
    skipped until the software actually begins to do something useful.
    """
    
    @defer.inlineCallbacks
    def setUp(self):
        log.debug("Starting Setup...")
        server_url = 'https://alice:1234@10.180.80.200:7080'
        #server_url = 'https://alice:1234@128.208.234.246:7080'
        yield self._start_container()
        services = [
            {'name':'OMS_Driver',
             'module':'ion.agents.platformagents.oms_driver',
             'class':'OMSDriver',
             'spawnargs':{'serverurl':server_url, 'maxconnections':20}
                }
            ]
        self.sup = yield self._spawn_processes(services)
        
        self.driver_pid = yield self.sup.get_child_id('OMS_Driver')
        log.debug("Driver pid %s" % (self.driver_pid))
        
        self.proc_client = ProcessClient(proc=self.sup, target=self.driver_pid)
    
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_simple_call(self):
        raise unittest.SkipTest('Integration test requires VPN into RSN')
        (content, headers, message) = \
            yield self.proc_client.rpc_send('connect', ('Server', 'if1Speed'))
        log.debug("*** content: %s, message: %s", content, message)
        self.assertTrue(isinstance(content, dict))
        self.assertTrue(len(content)>0)
        # Wish there were a better assert here, but the system has "live" or
        # at least variable data.