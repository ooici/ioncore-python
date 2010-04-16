#!/usr/bin/env python

"""
@file ion/services/coi/datastore.py
@author Michael Meisinger
@package ion.services.coi service for storing and retrieving stateful data objects.
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu

store = Store()

datastore = Store()

receiver = Receiver(__name__)
selfid = None

class Store:
    def started(self):
        return(hasattr(self, 'kvs'))

    def start(self):
        if self.started():
            return

        log.msg('Connecting to Cassandra...')
        cass_list = ['localhost:9160']
        self.client = pycassa.connect(cass_list)
        self.kvs = pycassa.ColumnFamily(self.client, 'Datasets', 'Catalog')
        log.msg('connected OK.')

    def get(self, key):
        if not self.started():
            log.err('Not connected!')
            return None
        try:
            val = self.kvs.get(key)
            log.msg('Key "%s":"%s"' % (key, val))
            return(val['value'])
        except:
            log.msg('Key "%s" not found' % key)
            return(None)

    def put(self, key, value):
        if not self.started():
            log.err('Not connected!')
            return None
        log.msg('writing key %s value %s' % (key, value))
        self.kvs.insert(key, {'value' : str(value)})

    def query(self, regex):
        log.err('Missing code')
        return None

    def delete(self, key):
        log.err('Missing code')
        return None

datastore = Store()

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    datastore.start()
    datastore.put('datastore', id)

def receive(content, msg):
    """
    content - content can be anything (list, tuple, dictionary, string, int, etc.)

    For this implementation, 'content' will be a dictionary:
        content = {
            "op": "operation name here",
            "args": {'key1':'arg1', 'key2':'arg2'}
        }
    """
    try:
        cmd = content['op']
        key = content['args']['key']
    except KeyError:
        log.err('Error parsing message!')
        return

    if cmd == 'PUT':
        value = content['args']['value']
        datastore.put(key, value)
    elif cmd == 'GET':
        log.msg(datastore.get(key))
    elif cmd == 'START':
        log.msg('Start command received')
    else:
        log.err('Unknown command ' + cmd)


receiver.handle(receive)

def pfh_test():
    start()
    receive({'op':'PUT','args':{'key':'key1','value':'val1'}}, None)
    receive({'op':'GET','args':{'key':'key1'}}, None)


def test_put():
	start()
	receiver.send(selfid, {'op':'PUT','args':{'key':'key1','value':'val1'}})
	# send(1,{'op':'PUT','args':{'key':'key1','value':'val1'}})

def test_get():
    receiver.send(selfid, {'op':'GET','args':{'key':'key1'}})
    # send(1, {'op':'GET','args':{'key':'key1'}})
