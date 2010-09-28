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
        pass

    @defer.inlineCallbacks
    def op_funstuff(self, content, headers, msg):
        pass
    


# Spawn of the process using the module name
factory = ProtocolFactory(ExchangeManagementService)



class ExchangeManagementClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'exchange_service'
        BaseServiceClient.__init__(self, proc, **kwargs)

