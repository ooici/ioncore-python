#!/usr/bin/env python

"""
@file ion/services/sa/data_acquisition.py
@author Michael Meisinger
@brief service for data acquisition
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.data.dataobject import DataObject
from ion.services.base_service import BaseService, BaseServiceClient

from ion.resources.sa_resource_descriptions import InstrumentResource, DataProductResource
from ion.services.sa.instrument_registry import InstrumentRegistryClient
from ion.services.sa.data_product_registry import DataProductRegistryClient

class InstrumentManagementService(BaseService):
    """
    Instrument management service interface.
    This service provides overall coordination for instrument management within
    an observatory context. In particular it coordinates the access to the
    instrument and data product registries and the interaction with instrument
    agents.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='instrument_management',
                                          version='0.1.0',
                                          dependencies=[])

    def slc_init(self):
        self.irc = InstrumentRegistryClient(proc=self)
        self.dprc = DataProductRegistryClient(proc=self)

    @defer.inlineCallbacks
    def op_create_new_instrument(self, content, headers, msg):
        """
        Service operation: Accepts a dictionary containing user inputs and updates the instrument
        registry.
        """
        userInput = content['userInput']

        newinstrument = InstrumentResource.create_new_resource()

        if 'direct_access' in userInput:
            newinstrument.direct_access = userInput['direct_access']

        if 'instrumentID' in userInput:
            newinstrument.instrumentID = userInput['instrumentID']

        if 'manufacturer' in userInput:
            newinstrument.manufacturer = userInput['manufacturer']

        if 'model' in userInput:
            newinstrument.model = userInput['model']

        if 'serial_num' in userInput:
            newinstrument.serial_num = userInput['serial_num']

        if 'fw_version' in userInput:
            newinstrument.fw_version = userInput['fw_version']

        instrument_res = yield self.irc.register_instrument_instance(newinstrument)
        print "****1"
        yield self.reply_ok(msg, instrument_res.encode())
        print "****2"

    @defer.inlineCallbacks
    def op_register_data_product(self, content, headers, msg):
        """
        Service operation: Accepts a dictionary containing user inputs and
        updates the data product registry.
        """
        dataProductInput = content['dataProductInput']

        newdp = DataProductResource.create_new_resource()
        if 'dataformat' in dataProductInput:
            newdp.dataformat = dataProductInput['dataformat']

        res = yield self.dprc.register_data_product(newdp)
        ref = res.reference(head=True)

        yield self.reply_ok(msg, res.encode())

class InstrumentManagementClient(BaseServiceClient):
    """
    Class for the client accessing the instrument management service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_management"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_new_instrument(self, userInput):
        reqcont = {}
        reqcont['userInput'] = userInput

        (content, headers, message) = yield self.rpc_send('create_new_instrument',
                                                          reqcont)
        print "***3"
        defer.returnValue(DataObject.decode(content['value']))

    @defer.inlineCallbacks
    def register_data_product(self, dataProductInput):
        reqcont = {}
        reqcont['dataProductInput'] = dataProductInput

        (content, headers, message) = yield self.rpc_send('register_data_product',
                                                          reqcont)
        defer.returnValue(DataObject.decode(content['value']))

# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentManagementService)
