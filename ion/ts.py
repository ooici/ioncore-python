#!/usr/bin/env python

"""
@file ion/ts.py
@author Michael Meisinger
@brief test service with short packet and module name
"""


from twisted.internet import defer

from magnet.spawnable import Receiver

from magnet.spawnable import spawn
from magnet.store import Store

from ion.core import bootstrap
from ion.core import base_process
import ion.util.procutils as pu

store = Store()

receiver = Receiver(__name__)

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    yield store.put('ts', id)

    yield bootstrap.start()
    yield test_datastore()

@defer.inlineCallbacks
def test_datastore():
    print "===================================================================="
    print "===================================================================="
    print "Testing datastore"

    to = yield base_process.procRegistry.get('datastore')

    print "Send PUT to: ",to
    yield pu.send_message(receiver, '', to, 'put', {'key':'obj1','value':'999'}, {'some':'header'})

    print "===================================================================="
    print "Send GET to: ",to
    yield pu.send_message(receiver, '', to, 'get', {'key':'obj1'}, {})

def receive(content, msg):
    print 'in receive ', content, msg

receiver.handle(receive)

# Called as 
if __name__ == '__main__':
    pass