#!/usr/bin/env python

"""
@file ion/services/coi/test/test_host_status_rpc.py
@author Brian Fox
@brief test rpc portion of the host status classes
"""

import logging
logging = logging.getLogger(__name__)

from ion.test.iontest import IonTestCase
from ion.services.coi.hostsensor.readers import HostReader


class HostStatusTest(IonTestCase):
    """
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass
   
    
    
    # Test SNMP
    
    def test_DoomedSnmpReader(self):
        reader = HostReader('localhost', 180, 'ccagent', 'ooicinet',timeout=0.5, retries=0)
        status = reader.getAll()
        self.assertFalse(status['SupportsSNMP'])
        self.assertFalse(status['SupportsRFC1213'])
        self.assertFalse(status['SupportsRFC2790'])

        
    def test_GoodSnmpReader(self):
        reader = HostReader('localhost', 161, 'ccagent', 'ooicinet')
        status = reader.getAll()
        self.assertTrue(status['SupportsSNMP'])
        self.assertTrue(status['SupportsRFC1213'])
        self.assertTrue(status['SupportsRFC2790'])
        print reader.getAllPrettyPrint()
    


