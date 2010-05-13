#!/usr/bin/env python

"""
@file ion/util/procutils.py
@author Michael Meisinger
@brief  utility helper functions for processes in capability containers
"""

import sys
import traceback
import re
from datetime import datetime
import time
import logging
from twisted.internet import defer, reactor
from magnet.container import Id
from magnet.store import Store

def log_exception(msg=None, e=None):
    """Logs a recently caught exception and prints traceback
    """
    if msg and e:
        logging.error(msg + " " + repr(e))
    elif msg:
        logging.error(msg)
    (etype, value, trace) = sys.exc_info()
    traceback.print_tb(trace)

def log_attributes(obj):
    """Print an object's attributes
    """
    lstr = ""
    for attr, value in obj.__dict__.iteritems():
        lstr = lstr + str(attr) + ": " +str(value) + ", "
    logging.info(lstr)

def log_message(proc, body, msg):
    """Log an incoming message with all headers
    """
    #mkeys = sorted(msg.__dict__.keys)
    mkeys = msg.__dict__.keys().sort()
    lstr = ""
    lstr += "===Message=== RECEIVED @" + str(proc) + "\n"
    amqpm = str(msg._amqp_message)
    # Cut out the redundant or encrypted AMQP body to make log shorter
    amqpm = re.sub("body='(\\\\'|[^'])*'","*BODY*", amqpm)
    lstr += '---AMQP--- ' + amqpm
    lstr += "\n---CARROT--- "
    for attr,value in msg.__dict__.iteritems():
        if attr == '_amqp_message': pass
        elif attr == 'body': pass
        elif attr == '_decoded_cache': pass
        else:
            lstr += str(attr) + ": " +str(value) + ", "
    lstr += "\n---HEADERS--- "
    mbody = {}
    mbody.update(body)
    content = mbody.pop('content')
    lstr += str(mbody)
    lstr += "\n---CONTENT---\n"
    lstr += str(content)
    lstr += "\n============="
    logging.info(lstr)

def get_process_id(long_id):
    """Returns the instance part of a long process id
    """
    if long_id == None:
        return None
    parts = str(long_id).rpartition('.')
    if parts[1] != '':
        procId = Id(parts[2],parts[0])
    else:
        procId = Id(long_id)
    return procId

@defer.inlineCallbacks
def send_message(receiver, send, recv, operation, content, headers):
    """Constructs a standard message with standard headers

    @param operation the operation (performative) of the message
    @param headers dict with headers that may override standard headers
    """
    msg = {}
    # The following headers are FIPA ACL Message Format based
    # Exchange name of sender, receiver, reply-to
    msg['sender'] = str(send)
    msg['receiver'] = str(recv)
    msg['reply-to'] = str(send)
    # Wire form encoding, such as 'json', 'fudge', 'XDR', 'XML', 'custom'
    msg['encoding'] = 'json'
    # Language of the format specification
    msg['language'] = 'ion1'
    # Identifier of a registered format specification (i.e. message schema)
    msg['format'] = 'raw'
    # Ontology associated with the content of the message
    msg['ontology'] = ''
    # Conversation instance id
    msg['conv-id'] = ''
    # Conversation type id
    msg['protocol'] = ''
    msg['us'] = str(currenttime_ms())
    #msg['reply-with'] = ''
    #msg['in-reply-to'] = ''
    #msg['reply-by'] = ''
    # Sender defined headers are updating the default headers set above.
    msg.update(headers)
    # Operation of the message, aka performative, verb, method
    msg['op'] = operation
    # The actual content
    msg['content'] = content
    logging.info("Send message op="+operation+" to="+str(recv))
    try:
        yield receiver.send(recv, msg)
    except StandardError, e:
        log_exception("Send error: ", e)
    else:
        logging.info("Message sent!")

def dispatch_message(payload, msg, dispatchIn, conv=None):
    """
    Dispatches a message by operation in a given class.
    payload = {
        "op": "operation name here",
        "content": ('arg1', 'arg2')
    }
    """
    try:
        log_message(__name__, payload, msg)

        if "op" in payload:
            op = payload['op']
            logging.info('dispatch_message() operation=' + str(op))

            content = payload.get('content','')
            opname = 'op_' + str(op)

            # dynamically invoke the operation in the given class
            if hasattr(dispatchIn, opname):
                opf = getattr(dispatchIn, opname)
                return defer.maybeDeferred(opf, content, payload, msg)
            elif hasattr(dispatchIn,'op_none'):
                return defer.maybeDeferred(dispatchIn.op_none, content, payload, msg)
            else:
                logging.error("Receive() failed. Cannot dispatch to catch")
        else:
            logging.error("Invalid message. No 'op' in header", payload)
    except StandardError, e:
        log_exception('Exception while dispatching: ',e)

id_seqs = {}
def create_unique_id(ns):
    """Creates a unique id for the given name space based on sequence counters.
    """
    if ns == None: ns = ':'
    nss = str(ns)
    if nss in id_seqs: nsc = int(id_seqs[nss]) +1
    else: nsc = 1
    id_seqs[nss] = nsc
    return nss + str(nsc)
    
    
def get_class(qualclassname, mod=None):
    """Imports module and class and returns class object.
    
    @param qualclassname  fully qualified classname, such as
        ion.data.dataobject.DataObject if module not given, otherwise class name
    @param mod instance of module
    @retval instance of 'type', i.e. a class object
    """
    if mod:
        clsname = qualclassname
    else:
        # Cut the name apart into package, module and class names
        qualmodname = qualclassname.rpartition('.')[0]
        modname = qualmodname.rpartition('.')[2]
        clsname = qualclassname.rpartition('.')[2]
        mod = get_module(qualmodname)

    cls = getattr(mod, clsname)
    #logging.debug('Class: '+str(cls))
    return cls

get_modattr = get_class

def get_module(qualmodname):
    """Imports module and returns module object
    @param fully qualified modulename, such as ion.data.dataobject
    @retval instance of types.ModuleType or error
    """
    package = qualmodname.rpartition('.')[0]
    modname = qualmodname.rpartition('.')[2]
    #logging.info('get_module: from '+package+' import '+modname)
    mod = __import__(qualmodname, globals(), locals(), [modname])
    #logging.debug('Module: '+str(mod))
    return mod

def asleep(secs):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, None)
    return d

def currenttime():
    """
    @retval current UTC time as float with seconds in epoch and fraction
    """
    now = datetime.utcnow()
    return time.mktime(now.timetuple()) + now.microsecond / 1000000.0

def currenttime_ms():
    """
    @retval current UTC time as int with milliseconds in epoch
    """
    return int(currenttime() * 1000)
    
# Stuff for testing: Stubs, mock objects
fakeStore = Store()

class FakeMessage(object):
    """Instances of this object are given to receive functions and handlers
    by test cases, in lieu of carrot BaseMessage instances. Production code
    detects these and no send is done.
    """
    def __init__(self, payload=None):
        self.payload = payload
    
    @defer.inlineCallbacks
    def send(self, to, msg):
        self.sendto = to
        self.sendmsg = msg
        # Need to be a generator
        yield fakeStore.put('fake','fake')

class FakeSpawnable(object):
    def __init__(self, id=None):
        self.id = id if id else Id('fakec','fakep')

class FakeReceiver(object):
    """Instances of this object are given to send/spawn functions
    by test cases, in lieu of magnet Receiver instances. Production code
    detects these and no send is done.
    """
    def __init__(self, id=None):
        self.payload = None
        self.spawned = FakeSpawnable()

    @defer.inlineCallbacks
    def send(self, to, msg):
        self.sendto = to
        self.sendmsg = msg
        # Need to be a generator
        yield fakeStore.put('fake','fake')

