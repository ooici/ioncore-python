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

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('datastore', id)
    selfid = id

def op_put(args):
    print 'in "put" with ', args
    datastore.put(args['key'],args['value'])

@defer.inlineCallbacks
def op_get(args, replyto):
    print 'in "get" with ', args, replyto
    value = yield datastore.get(args['key'])
    print "GET '"+args['key']+"'='"+value+"'"

    receiver.send(pu.get_process_id(replyto), {'from':'id', 'return-value':value})


def receive(content, msg):
    """
    content - content can be anything (list, tuple, dictionary, string, int, etc.)

    For this implementation, 'content' will be a dictionary:
        content = {
            "op": "operation name here",
            "args": {'key1':'arg1', 'key2':'arg2'}
        }
    """
    pu.log_message(__name__, content, msg)
    if "op" in content:
		if content["op"] == "START":
			print 'Start message received'
			return
		elif content["op"] == "GET":
			return op_get(content['content'],msg.reply_to)
		elif content["op"] == "PUT":
			return op_put(content['content'])
#		else:
#		    log.error("Receive() failed. Op call does not match a service", content)    
#	else:
    log.error("Receive() failed. Bad message", content)    

receiver.handle(receive)

def test_put():
	start()
	receiver.send(selfid, {'op':'PUT','args':{'key':'key1','value':'val1'}})
	# send(1,{'op':'PUT','args':{'key':'key1','value':'val1'}})
	
def test_get():
    receiver.send(selfid, {'op':'GET','args':{'key':'key1'}})
	# send(1, {'op':'GET','args':{'key':'key1'}})
	