#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/ingestion_service.py
@author Brian Fox
@brief service for managing exchanges, exchange points, and exchange spaces
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.coi.exchange.exchange_registry import ExchangeRegistryClient

from ion.resources.coi_resource_descriptions import ExchangeName

class ExchangeManagementService(BaseService):
    """
    Allows exchanges to be managed.
    @todo Skeleton
    """

    # Declaration of service
    declare = BaseService.service_declare(name='exchange_service',
                                          version='0.1.0',
                                          dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.erc = ExchangeRegistryClient(proc=self)



    def _populate_resource(self, userInput, resource):
        """
        This is an ugly technique to get around an even uglier laundry list
        of if-then declarations.
        """
        for k in userInput:
            expr = "resource.%s = str(userInput['%s'])"%(k,k)
            eval(expr)
            

    @defer.inlineCallbacks
    def op_create_new_exchangename(self, content, headers, msg):
        """
        Service operation: Creates a new exchange name from the 
        attributes found in content.
        """
        userInput = content['userInput']
        xname = ExchangeName.create_new_resource()
        self._populate_resource(userInput, xname)
        xn_res = yield self.erc.define_exchange_name(xname)
        yield self.reply_ok(msg, xname.encode())
        


# Spawn of the process using the module name
factory = ProtocolFactory(ExchangeManagementService)



class ExchangeManagementClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'exchange_service'
        BaseServiceClient.__init__(self, proc, **kwargs)

