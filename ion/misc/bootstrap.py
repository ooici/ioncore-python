#!/usr/bin/env python

"""
@file ion/misc/bootstrap.py
@author Michael Meisinger
@package ion.misc bootstrapping the system.
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.services.coi import datastore

store = Store()

receiver = Receiver(__name__)
started = False

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('bootstrap', id)
    
@defer.inlineCallbacks
def op_bootstrap():
    print "Bootstrapping now"
    started = True
    
    id = yield spawn(datastore)
    store.put('datastore', id)
    
    to = yield store.get('datastore')
    receiver.send(to, {'method':'PUT','args':{'key':'obj1','value':999}})

    replyto = yield store.get('bootstrap')
    receiver.send(to, {'method':'GET','args':{'key':'obj1','reply-to':replyto}})

def receive(content, msg):

    print 'in receive ', content
    if not started:
        op_bootstrap()

receiver.handle(receive)
