#!/usr/bin/env python

"""
@file ion/services/coi/test/test_host_status_rpc.py
@author Brian Fox
@brief test rpc portion of the host status classes
"""

import logging, json, subprocess, sys, socket
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.test.iontest import IonTestCase
from ion.services.coi.host_status.host_status import SnmpReader,HostStatus


class HostStatusTest(IonTestCase):
    """
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    # The chain is a little convoluted.  We start with a local SNMP server.  There
    # are a few things that can go wrong there (see below).  Then we bump up to a 
    # RCP server which is a little more than a wrapper to avoid twisted issues.
    # Then we end up with the "client-server" propagation, a service that polls
    # the RPC server and places the results onto the AMQP backbone.
    
    
    # Testing SNMP
    
    def test_DeliberatelyBadSnmpReader(self):
        hs = HostStatus('localhost', 180, 'ccagent', 'ooicinet',timeout=0.5, retries=0)
        all = json.dumps(hs.getAll(), indent=4)
        print all
        
    def test_SnmpReader(self):
        hs = HostStatus('localhost', 161, 'ccagent', 'ooicinet')
        all = json.dumps(hs.getAll(), indent=4)
        print all
    
    
    # Testing RPC wrapper
                
    def test_RPCServer(self):
        """
        RPC Server is started as a process, testing that it starts and shutsdown
        gracefully.
        """
        try:
            p = subprocess.Popen(
                (sys.executable,"ion/services/coi/host_status/host_status_server.py"),
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
                )
        except socket.error:
            print "Socket already in use"
            p.kill()
            
        status = p.stdout.readline()
        p.kill()


