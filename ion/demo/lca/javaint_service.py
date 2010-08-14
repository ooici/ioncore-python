#!/usr/bin/env python

"""
@file ion/demo/javaint_service.py
@author Michael Meisinger
@brief Java service integration service for R1 LCA.
"""
import ast

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class JavaIntegrationService(BaseService):
    """
    Example service interface
    """
    # Declaration of service
    declare = BaseService.service_declare(name='javaint',
                                          version='0.1.0',
                                          dependencies=[])

    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('JavaIntegrationService.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        #self.instruments = ["SBE49_1","SBE49_2"]
        #self.instruments = {'Instrument1':{'a':'1','b':'2','c':'3'},'Instrument2':{'e':'1','f':'2','g':'e'}}
        #self.instruments = [{'id':343,'name':'Instrument3','manufacturer':'Sony','modelNumber':'1.3','instrumentType':'aquatic','versionNumber':'232'},{'id':938,'name':'Instrument1','manufacturer':'2','modelNumber':'3','instrumentType':'a2','versionNumber':'za'},{'id':1029,'name':'Instrument2','manufacturer':'242','modelNumber':'333','instrumentType':'a2asd','versionNumber':'asdf'}]
        self.instruments = [{'name':'Instrument3','manufacturer':'Sony','modelNumber':'1.3','instrumentType':'aquatic','versionNumber':'232'},{'name':'Instrument1','manufacturer':'2','modelNumber':'3','instrumentType':'a2','versionNumber':'za'},{'name':'Instrument2','manufacturer':'242','modelNumber':'333','instrumentType':'a2asd','versionNumber':'asdf'}]
        self.datasets = [{'name':'Data product 1','instrumentType':'Atomic'},{'name':'Data product 2','instrumentType':'Nuclear'},{'name':'Data product 3','instrumentType':'Data Collection'}]
        self.services = [{'name':'Instrument Registry Service','status':'active'},{'name':'Data Product Service','status':'active'},{'name':'Service X','status':'active'}]

    @defer.inlineCallbacks
    def op_hello(self, content, headers, msg):
        logging.info('op_hello: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})

    @defer.inlineCallbacks
    def op_list_all_instruments(self, content, headers, msg):
        logging.info('op_list_all_instruments: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.instruments})

    @defer.inlineCallbacks
    def op_register_instrument(self, content, headers, msg):
        logging.info('op_register_instrument: '+ str(content))

        newInstrument = ast.literal_eval(str(content))

        # get instrument name
        self.instruments.append(newInstrument)
        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.instruments})

    @defer.inlineCallbacks
    def op_list_all_datasets(self, content, headers, msg):
        logging.info('op_list_all_datasets: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.datasets}    )

    @defer.inlineCallbacks
    def op_register_dataset(self, content, headers, msg):
        logging.info('op_register_dataset: '+ str(content))

        newDataset = ast.literal_eval(str(content))

        self.datasets.append(newDataset)

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.datasets})

    @defer.inlineCallbacks
    def op_list_all_services(self, content, headers, msg):
        logging.info('op_list_all_services: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.services})
        
    @defer.inlineCallbacks
    def op_register_service(self, content, headers, msg):
        logging.info('op_register_service: '+ str(content))

        newService = ast.literal_eval(str(content))

        self.services.append(newService)

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.services})    

# Spawn of the process using the module name
factory = ProtocolFactory(JavaIntegrationService)
