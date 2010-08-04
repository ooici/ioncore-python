#!/usr/bin/env python

"""
@file ion/demo/javaint_service.py
@author Michael Meisinger
@brief Java service integration service for R1 LCA.
"""

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
        self.instruments = ["SBE49_1","SBE49_2"]
        self.datasets = []
        self.services = {'Service1':{'key1':'val1','key2':'val2'},'Service2':{'key1':'val3','key2':'val4'}}

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
        logging.info('op_register_instrument: '+str(content))

        instname = str(content['InstName'])
        if not instname in self.instruments:
            self.instruments.append(instname)

        # The following line shows how to reply to a message
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_list_all_datasets(self, content, headers, msg):
        logging.info('op_list_all_datasets: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.datasets})

    @defer.inlineCallbacks
    def op_register_dataset(self, content, headers, msg):
        logging.info('op_register_dataset: '+str(content))

        dsname = str(content['DSName'])
        if not dsname in self.datasets:
            self.datasets.append(dsname)

        # The following line shows how to reply to a message
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_list_all_services(self, content, headers, msg):
        logging.info('op_list_all_services: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':self.services})

# Spawn of the process using the module name
factory = ProtocolFactory(JavaIntegrationService)
