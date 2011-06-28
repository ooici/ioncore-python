#!/usr/bin/env python

"""
@file ion/play/hello_resource.py
@author Brian Fox
@brief Exchange Management client intended for use in the CC Agent
"""

import ion.util.ionlog
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core import ioninit
from twisted.internet import defer

import ion.services.coi.exchange.resource_wrapper as res_wrapper
from ion.services.coi.exchange.resource_wrapper import ServiceHelper, ClientHelper
from ion.services.coi.exchange.broker_controller import BrokerController
from ion.services.coi.exchange.exchange_types import ExchangeTypes

CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)



class ExchangeManagementClient(ServiceClient):
    
    def __init__(self, proc=None, **kwargs):
        log.info("ExchangeManagementService.slc_init(...)")
        self.proc = proc
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "exchange_management"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def slc_init(self):
        log.info("ExchangeManagementService.slc_init(self)")
        self.helper = ClientHelper(self.proc)
        self.controller = BrokerController(self.proc)
        self.exchange_types = ExchangeTypes(self.controller)
        yield self.controller.start()
        
        self.xs = {}
        self.xn = {}
        


    @defer.inlineCallbacks
    def slc_deactivate(self):
        log.info("ExchangeManagementService.slc_terminate(self)")
        yield self.controller.stop()
        
        
