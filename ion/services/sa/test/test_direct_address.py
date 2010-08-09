#!/usr/bin/env python

"""
@file ion/play/test/test_data_acquisition.py
@test ion.services.sa.data_acquisition Example unit tests for sample code.
@author Michael Meisinger
"""

from twisted.internet import defer

from ion.services.sa.direct_access import DAInstrumentRegistry, DADataProductRegistry
from ion.test.iontest import IonTestCase

from twisted.trial import unittest

class Instance():
     instrument = {}
     dataproduct = {}

class DataAcquisitionTest(IonTestCase):
    """
    Testing example data acquisition service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        services = [
            {'name':'resourceregistry1','module':'ion.services.sa.instrument_registry','class':'InstrumentRegistryService'}]
        sup = yield self._spawn_processes(services)
        self.IR = DAInstrumentRegistry(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

         
    @defer.inlineCallbacks
    def test_direct_access(self):
            
        """
        Switches direct_access mode to ON in the instrument registry.
        """
            
        userUpdate = {'direct_access' : "on"}
        Instance.instrument = yield self.IR.register_instrument(userUpdate)
        self.assertEqual(Instance.instrument.direct_access, "on") #change made

        

