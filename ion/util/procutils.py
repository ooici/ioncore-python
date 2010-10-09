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
    Log an inbound message with all headers unless quiet attribute set.
    @param msg  carrot BaseMessage instance
    """
    body = msg.payload
    lstr = ""
    procname = str(body.get('receiver',None))
    lstr += "===Message=== receiver=%s op=%s===" % (procname, body.get('op', None))
    if body.get('quiet', False):
        lstr += " (Q)"
    else:
        amqpm = str(msg._amqp_message)
        # Cut out the redundant or encrypted AMQP body to make log shorter
        amqpm = re.sub("body='(\\\\'|[^'])*'","*BODY*", amqpm)
        lstr += '\n---AMQP--- ' + amqpm + "; "
        for attr in sorted(msg.__dict__.keys()):
            value = msg.__dict__.get(attr)
            if attr == '_amqp_message' or attr == 'body' or \
                    attr == '_decoded_cache' or attr == 'backend':
                pass
            else:
                lstr += "%s=%r, " % (attr, value)
        lstr += "\n---ION HEADERS--- "
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
    """
    @brief Do a reactor-safe sleep call. Call with yield to block until done.
    @param secs Time, in seconds
    @retval Deferred whose callback will fire after time has expired
    """
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

