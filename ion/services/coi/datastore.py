#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author Michael Meisinger
@brief service for storing and retrieving stateful data objects.
"""

import logging
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient


class DatastoreService(BaseService):
    datastore = Store()
    #datastore = CassandraStore()

    # Declaration of service
    declare = BaseService.service_declare(name='datastore', version='0.1.0', dependencies=[])

    def slc_init(self):
        pass
        #self.datastore.start()

    @defer.inlineCallbacks
    def op_put(self, content, headers, msg):
        """Service operation: Store object in the datastore
        """
        key = content.get('key','')
        value = content.get('value','')
        logging.info('Datastore.put('+key+','+value+')')
        yield self.datastore.put(key,value)

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """Service operation: Retrieve object from the datastore
        """
        key = content.get('key','')
        logging.info('Datastore.get('+key+')')

        value = yield self.datastore.get(key)
        logging.info('Datastore.get('+key+') = '+str(value))
        yield self.reply_message(msg, 'result', {'value':value}, {})


# Spawn of the process using the module name
factory = ProtocolFactory(DatastoreService)

"""
from ion.services.coi import datastore as d
spawn(d)
send (1, {'op':'put','content':{'key':'k1','value':'v'}})
"""
