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

store = Store()

datastore = Store()

receiver = Receiver(__name__)

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('datastore', id)

def op_put(args):
    print 'in "put" with ', args
    datastore.put(args['key'],args['value'])

@defer.inlineCallbacks
def op_get(args):
    print 'in "get" with ', args
    value = yield datastore.get(args['key'])
    receiver.send(args['reply-to'], {'from':'id', 'return-value':value})


def receive(content, msg):
    """
    content - content can be anything (list, tuple, dictionary, string, int, etc.)

    For this implementation, 'content' will be a dictionary:
        content = {
            "method": "method name here",
            "args": ('arg1', 'arg2')
        }
    """
    print 'in receive ', content, msg
    try:
        if "method" in content:
            if content["method"] == "GET":
                return op_get(content['args'])
        if "method" in content:
            if content["method"] == "PUT":
                return op_put(content['args'])
        else:
            raise NameError
    except Exception:
        log.error("Receive() failed. Method call does not match a service",
                content)

receiver.handle(receive)
