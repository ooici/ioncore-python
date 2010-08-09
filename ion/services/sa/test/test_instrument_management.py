#!/usr/bin/env python

"""
@file ion/play/test/test_data_acquisition.py
@test ion.services.sa.data_acquisition Example unit tests for sample code.
@author Michael Meisinger
"""

from twisted.internet import defer

from ion.services.sa.instrument_management import DAInstrumentRegistry, DADataProductRegistry
from ion.test.iontest import IonTestCase

from twisted.trial import unittest

class Instance():
     instrument = {}
     dataproduct = {}

class DataAcquisitionTest(IonTestCase):
    """
    Testing instrument management
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        services = [
            {'name':'resourceregistry1','module':'ion.services.sa.instrument_registry','class':'InstrumentRegistryService'}]
        sup = yield self._spawn_processes(services)
        self.IR = DAInstrumentRegistry(proc=sup)
        
        services = [
            {'name':'resourceregistry1','module':'ion.services.sa.data_product_registry','class':'DataProductRegistryService'}]
        sup = yield self._spawn_processes(services)

        self.DP = DADataProductRegistry(proc=sup)
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_management_service(self):
        
        """
        Accepts an dictionary containing updates to the instrument registry.  
        Updates are made to the registries.
        """ 
        
        userUpdate = {'manufacturer' : "SeaBird Electronics", 
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}
        instrumentID = "500"
        userUpdate['instrumentID'] = instrumentID
                
        Instance.instrument = yield self.IR.register_instrument(userUpdate)
        
        dataProductInput = {'dataformat' : "binary"}
        
        Instance.dataproduct = yield self.DP.register_data_product(dataProductInput)
        
        self.assertEqual(Instance.instrument.instrumentID, "500")
        self.assertEqual(Instance.instrument.manufacturer, "SeaBird Electronics")
        self.assertEqual(Instance.instrument.model, "unknown model") #change made
        self.assertEqual(Instance.instrument.serial_num, "1234")
        self.assertEqual(Instance.instrument.fw_version, "1")
        self.assertEqual(Instance.dataproduct.dataformat, "binary")
        
    

        

