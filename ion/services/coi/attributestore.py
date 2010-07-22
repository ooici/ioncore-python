#!/usr/bin/env python

"""
@file ion/services/coi/attributestore.py
@author Michael Meisinger
@author David Stuebe
@brief service for storing and retrieving key/value pairs.
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.core import ioninit
from ion.core.base_process import ProtocolFactory
from ion.data.backends import store_service
from ion.services.base_service import BaseService, BaseServiceClient
import ion.util.procutils as pu

CONF = ioninit.config(__name__)

class AttributeStoreService(store_service.StoreService):
    """
    Service to store and retrieve key/value pairs.
    The Implementation is in ion.data.backends.store_service
    """
    # Declaration of service
    declare = BaseService.service_declare(name='attributestore',
                                          version='0.1.0',
                                          dependencies=[])

    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        
        self.spawn_args['backend_class'] = self.spawn_args.get('backend_class', CONF.getValue('backend_class', default='ion.data.store.Store'))
        self.spawn_args['backend_args'] = self.spawn_args.get('backend_args', CONF.getValue('backend_args', default={}))

        logging.info('AttributeStoreService.__init__()')



class AttributeStoreClient(store_service.StoreServiceClient):
    """
    Class for the client accessing the attribute store via Exchange
    The Implementation is in ion.data.backends.store_service
    The client provides the IStore interface
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "attributestore"
        BaseServiceClient.__init__(self, proc, **kwargs)

# Spawn of the process using the module name
factory = ProtocolFactory(AttributeStoreService)
