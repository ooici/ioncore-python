#!/usr/bin/env python

"""
@file ion/util/procutils.py
@author Michael Meisinger
@brief  utility helper functions for processes in capability containers
"""

import sys
import traceback
import re
import time
import uuid

from twisted.internet import defer, reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
from ion.data.store import Store

def log_exception(msg=None, e=None):
    """
    Logs a recently caught exception and prints traceback
    """
    if msg and e:
        log.error("%s %r" % (msg, e))
    elif msg:
        log.error(msg)
    (etype, value, trace) = sys.exc_info()
    traceback.print_tb(trace)

def log_attributes(obj):
    """
    Print an object's attributes
    """
    lstr = ""
    for attr, value in obj.__dict__.iteritems():
        lstr = lstr + str(attr) + ": " +str(value) + ", "
    log.info(lstr)

def log_message(msg):
    """
    Log an incoming message with all headers unless quiet attribute set.
    @param msg  carrot BaseMessage instance
    """
    body = msg.payload
    lstr = ""
    procname = str(body.get('receiver',None))
    lstr += "===Message=== receiver=%s op=%s===" % (procname, body.get('op', None))
    if body.get('quiett', False):
        lstr += " (Q)"
    else:
        amqpm = str(msg._amqp_message)
        # Cut out the redundant or encrypted AMQP body to make log shorter
        amqpm = re.sub("body='(\\\\'|[^'])*'","*BODY*", amqpm)
        lstr += '\n---AMQP--- ' + amqpm
        lstr += "\n---CARROT--- "
        for attr in sorted(msg.__dict__.keys()):
            value = msg.__dict__.get(attr)
            if attr == '_amqp_message' or attr == 'body' or attr == '_decoded_cache':
                pass
            else:
                lstr += "%s=%r, " % (attr, value)
        lstr += "\n---HEADERS--- "
        mbody = dict(body)
        content = mbody.pop('content')
        for attr in sorted(mbody.keys()):
            value = mbody.get(attr)
            lstr += "%s=%r, " % (attr, value)
        lstr += "\n---CONTENT---\n"
        if type(content) is dict:
            for attr in sorted(content.keys()):
                value = content.get(attr)
                lstr += "%s=%r, " % (attr, value)
        else:
            lstr += repr(content)
        lstr += "\n============="
    log.debug(lstr)

def create_guid():
    """
    @retval Return global unique id string
    """
    return str(uuid.uuid4())

def get_process_id(some_id):
    """
    @brief Always returns an Id with qualified process id
    @param some_id any form of id, short or long, Id or str
    @retval Id with full process id
    """
    if some_id == None:
        return None
    parts = str(some_id).rpartition('.')
    if parts[1] != '':
        procId = Id(parts[2],parts[0])
    else:
        procId = Id(some_id)
    return procId

def get_scoped_name(name, scope):
    """
    Returns a name that is scoped.
    - scope='local': name prefixed by container id.
    - scope='system': name prefixed by system name.
    - scope='global': name unchanged.
    @param name name to be scoped
    @param scope  one of "local", "system" or "global"
    """
    scoped_name = name
    if scope == 'local':
        scoped_name =  str(ioninit.container_instance.id) + "." + name
    elif scope == 'system':
        scoped_name =  ioninit.sys_name + "." + name
    elif scope == 'global':
        pass
    else:
        raise RuntimeError("Unknown scope: %s" % scope)
    return  scoped_name

@defer.inlineCallbacks
def send(receiver, send, recv, operation, content, headers=None):
    """
    Constructs a standard message with standard headers and sends on given
    receiver.

    @param operation the operation (performative) of the message
    @param headers dict with headers that may override standard headers
    """
    msg = {}
    # The following headers are FIPA ACL Message Format based
    # Exchange name of sender (DO NOT SEND replies here)
    msg['sender'] = str(send)
    # Exchange name of message recipient
    msg['receiver'] = str(recv)
    # Exchange name for message replies
    msg['reply-to'] = str(send)
    # Wire form encoding, such as 'json', 'fudge', 'XDR', 'XML', 'custom'
    msg['encoding'] = 'json'
    # See ion.data.dataobject Serializers for choices
    msg['accept-encoding'] = ''
    # Language of the format specification
    msg['language'] = 'ion1'
    # Identifier of a registered format specification (i.e. message schema)
    msg['format'] = 'raw'
    # Ontology associated with the content of the message
    msg['ontology'] = ''
    # Conversation instance id
    msg['conv-id'] = ''
    # Conversation message sequence number
    msg['conv-seq'] = 1
    # Conversation type id
    msg['protocol'] = ''
    # Status code
    msg['status'] = 'OK'
    # Local timestamp in ms
    msg['ts'] = str(currenttime_ms())
    #msg['reply-with'] = ''
    #msg['in-reply-to'] = ''
    #msg['reply-by'] = ''
    # Sender defined headers are updating the default headers set above.
    if headers:
        msg.update(headers)
    # Operation of the message, aka performative, verb, method
    msg['op'] = operation
    # The actual content
    msg['content'] = content
    #log.debug("Send message op="+operation+" to="+str(recv))
    try:
        yield ioninit.container_instance.send(recv, msg)
    except Exception, ex:
        log_exception("Send error: ", ex)
    else:
        log.info("Message sent! to=%s op=%s" % (msg.get('receiver',None), msg.get('op',None)))
        #log.debug("msg"+str(msg))

def dispatch_message(payload, msg, dispatchIn, conv=None):
    """
    Dispatches a message by operation in a given class.
    Expected message content:
    body = {
        "op": "operation name here",
        "content": # Any valid type here (str, list, dict)
    }
    """
    try:
        log_message(msg)

        if "op" in payload:
            op = payload['op']
            content = payload.get('content','')
            opname = 'op_' + str(op)

            # dynamically invoke the operation in the given class
            if hasattr(dispatchIn, opname):
                opf = getattr(dispatchIn, opname)
                return defer.maybeDeferred(opf, content, payload, msg)
            elif hasattr(dispatchIn,'op_none'):
                return defer.maybeDeferred(dispatchIn.op_none, content, payload, msg)
            else:
                log.error("Receive() failed. Cannot dispatch to catch")
        else:
            log.error("Invalid message. No 'op' in header", payload)
    except Exception, ex:
        log_exception('Exception while dispatching: ',ex)

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
    #log.debug('Class: '+str(cls))
    return cls

get_modattr = get_class

def get_module(qualmodname):
    """Imports module and returns module object
    @param fully qualified modulename, such as ion.data.dataobject
    @retval instance of types.ModuleType or error
    """
    package = qualmodname.rpartition('.')[0]
    modname = qualmodname.rpartition('.')[2]
    #log.info('get_module: from '+package+' import '+modname)
    mod = __import__(qualmodname, globals(), locals(), [modname])
    #log.debug('Module: '+str(mod))
    return mod

def asleep(secs):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, None)
    return d

def currenttime():
    """
    @retval current UTC time as float with seconds in epoch and fraction
    """
    return time.time()

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
        self.id = id or Id('fakec','fakep')

class FakeReceiver(object):
    """Instances of this object are given to send/spawn functions
    by test cases, in lieu of ion.core.messaging.receiver.Receiver instances.
    Production code detects these and no send is done.
    """
    def __init__(self, id=None):
        self.payload = None
        self.process = FakeSpawnable()
        self.group = 'fake'

    @defer.inlineCallbacks
    def send(self, to, msg):
        self.sendto = to
        self.sendmsg = msg
        # Need to be a generator
        yield fakeStore.put('fake','fake')
