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

#import pycassa

import ion.util.procutils as pu
from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.base_svcproc import BaseServiceProcess

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class CassandraStore():
    def started(self):
        return(hasattr(self, 'kvs'))

    def start(self):
        if self.started():
            return

        logging.info('Connecting to Cassandra...')
        cass_list = ['localhost:9160']
        self.client = pycassa.connect(cass_list)
        self.kvs = pycassa.ColumnFamily(self.client, 'Datasets', 'Catalog')
        logging.info('connected OK.')

    def get(self, key):
        if not self.started():
            logging.error('Not connected!')
            return None
        try:
            val = self.kvs.get(key)
            logging.info('Key "%s":"%s"' % (key, val))
            return(val['value'])
        except:
            logging.info('Key "%s" not found' % key)
            return(None)

    def put(self, key, value):
        if not self.started():
            logging.error('Not connected!')
            return None
        logging.info('writing key %s value %s' % (key, value))
        self.kvs.insert(key, {'value' : str(value)})

    def query(self, regex):
        logging.error('Missing code')
        return None

    def delete(self, key):
        logging.error('Missing code')
        return None

class DatastoreService(BaseService):
    datastore = Store()
    #datastore = CassandraStore()
    
    def slc_init(self):
        self.datastore.start()

    @defer.inlineCallbacks
    def op_put(self, content, headers, msg):
        key = content.get('key','')
        value = content.get('value','')
        logging.info('Datastore.put('+key+','+value+')')
        yield self.datastore.put(key,value)

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        key = content.get('key','')
        logging.info('Datastore.get('+key+')')

        value = yield self.datastore.get(key)
        logging.info('Datastore.get('+key+') = '+str(value))
        replyto = msg.reply_to
        logging.info('Datastore.get() replyto='+replyto)
        yield pu.send_message(receiver, '', pu.get_process_id(replyto), 'result', {'value':value}, {})


# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = DatastoreService()

def receive(content, msg):
    pu.dispatch_message(content, msg, instance)

receiver.handle(receive)


def pfh_test():
    start()
    receive({'op':'put','content':{'key':'key1','value':'val1'}}, None)
    receive({'op':'get','content':{'key':'key1'}}, None)

"""
from ion.services.coi import datastore as d
spawn(d)
send (1, {'op':'put','content':{'key':'k1','value':'v'}})
"""