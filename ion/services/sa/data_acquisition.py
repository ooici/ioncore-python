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

class DataAcquisitionService(BaseService):
    """Data acquisition service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_acquisition', version='0.1.0', dependencies=[])
 
    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('DataAcquisitionService.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_acquire_block(self, content, headers, msg):
        """Service operation: Acquire an entire, fully described version of a
        data set.
        """
	logging.info('op_acquire_block: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_acquire_block_respond,'+str(content)}, {})

    @defer.inlineCallbacks
    def op_acquire_message(self, content, headers, msg):
        """Service operation: Acquire an increment of a data set.
        """
        
	logging.info('op_acquire_message: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_acquire_message_respond, '+str(content)}, {})

# Spawn of the process using the module name
factory = ProtocolFactory(DataAcquisitionService)

class DataAcquisitionServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the Data acquisition service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_acquisition"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def acquire_block(self, text='Hi there'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('acquire_block', text)
	#op_ will be added by default: eval call to run that
        logging.info('Acquire block Service reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def acquire_message(self, text='Hi there'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('acquire_message', text)
        logging.info('Acquire message Service reply: '+str(content))
        defer.returnValue(str(content))

class DAInstrumentRegistry(InstrumentRegistryClient):
    '''
    Updates the instrument registry.
    '''
    @defer.inlineCallbacks
    def register_instrument(self, userInput):
        '''
        Accepts a dictionary containing user inputs and updates the instrument
        registry.
        '''    
        
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
    '''
    Accepts a dictionary containing user inputs and updates the data product
    registry.
    '''    
    @defer.inlineCallbacks
    def register_data_product(self, dataProductInput):
        res = sa_resource_descriptions.DataProductResource.create_new_resource()
        res = yield self.register_data_product_type(res)
                
        ref = res.reference(head=True)
        
        res2 = yield self.get_data_product_type(ref)
        if 'dataformat' in dataProductInput:
            res2.dataformat = dataProductInput['dataformat']
        
        res2 = yield self.register_data_product_type(res2)
        
        res3 = yield self.get_data_product_type(ref)
        
        defer.returnValue(res3)
    
