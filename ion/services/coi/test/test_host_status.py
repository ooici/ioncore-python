#!/usr/bin/env python

"""
@file ion/services/coi/test/test_host_status_rpc.py
@author Brian Fox
@brief test rpc portion of the host status classes
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

import json
from ion.test.iontest import IonTestCase
from ion.services.coi.host_status import SnmpReader,HostStatus

class HostStatusRPCTest(IonTestCase):
    """
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass
    
    def test_SnmpReader(self):
        hs = HostStatus('localhost', 161, 'ccagent', 'ooicinet')
        all = json.dumps(hs.getAll(), indent=4)

    def test_DeliberatelyBadSnmpReader(self):
        hs = HostStatus('localhost', 180, 'ccagent', 'ooicinet')
        all = json.dumps(hs.getAll(), indent=4)



