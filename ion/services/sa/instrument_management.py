#!/usr/bin/env python

"""
@file ion/services/sa/data_acquisition.py
@author Michael Meisinger
@brief service for data acquisition
"""


import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.data.datastore.datastore_service import DataStoreServiceClient

from ion.resources import sa_resource_descriptions
from ion.services.sa.instrument_registry import InstrumentRegistryClient
from ion.services.sa.data_product_registry import DataProductRegistryClient


class DAInstrumentRegistry(InstrumentRegistryClient):
    """
    Updates the instrument registry.
    """
    @defer.inlineCallbacks
    def register_instrument(self, userInput):
        """
        Accepts a dictionary containing user inputs and updates the instrument
        registry.
        """   
        
        res = sa_resource_descriptions.InstrumentResource.create_new_resource()
        res = yield self.register_instrument_type(res)
        
       
        
        ref = res.reference(head=True)
        
        res2 = yield self.get_instrument_type(ref)
        
        if 'direct_access' in userInput:
            res2.direct_access = userInput['direct_access']
            
        if 'instrumentID' in userInput:
            res2.instrumentID = userInput['instrumentID']
        
        if 'manufacturer' in userInput:
            res2.manufacturer = userInput['manufacturer']
            
        if 'model' in userInput:
            res2.model = userInput['model']
            
        if 'serial_num' in userInput:
            res2.serial_num = userInput['serial_num']
            
        if 'fw_version' in userInput:
            res2.fw_version = userInput['fw_version']
            
        
        res2 = yield self.register_instrument_type(res2)
        
        res3 = yield self.get_instrument_type(ref)
        
        defer.returnValue(res3)
        
class DADataProductRegistry(DataProductRegistryClient):
    """
    Updates the data product registry
    """  
    @defer.inlineCallbacks
    def register_data_product(self, dataProductInput):
        """
        Accepts a dictionary containing user inputs and updates the data product
        registry.
        """  
        res = sa_resource_descriptions.DataProductResource.create_new_resource()
        res = yield self.register_data_product_type(res)
                
        ref = res.reference(head=True)
        
        res2 = yield self.get_data_product_type(ref)
        if 'dataformat' in dataProductInput:
            res2.dataformat = dataProductInput['dataformat']
        
        res2 = yield self.register_data_product_type(res2)
        
        res3 = yield self.get_data_product_type(ref)
        
        defer.returnValue(res3)
    
