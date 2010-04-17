#!/usr/bin/env python

"""
@file ion/ts.py
@author Michael Meisinger
@package ion  test service with short packet and module name
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.core import bootstrap
import ion.util.procutils as pu

store = Store()

receiver = Receiver(__name__)

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('ts', id)
    
    yield bootstrap.op_bootstrap()
    yield test_datastore()

@defer.inlineCallbacks
def test_datastore():
    print "===================================================================="
    print "===================================================================="
    print "Testing datastore"

    to = yield bootstrap.store.get('datastore')
    
    print "Send PUT to: ",to
    pu.send_message(receiver, '', to, 'PUT', {'key':'obj1','value':'999'}, {'some':'header'})

    print "===================================================================="
    print "Send GET to: ",to
    pu.send_message(receiver, '', to, 'GET', {'key':'obj1'}, {})

def receive(content, msg):
    print 'in receive ', content, msg
      
receiver.handle(receive)
