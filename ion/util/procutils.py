#!/usr/bin/env python

"""
@file ion/util/procutils.py
@author Michael Meisinger
@brief  utility helper functions for processes in capability containers
"""

import sys, traceback, re
import logging
from twisted.python import log
from twisted.internet import defer
from magnet.container import Id

from ion.core import ionconst as ic

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
    amqpm = re.sub("body='[^']*'","*BODY*", amqpm)
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

def send_message(receiver, send, recv, operation, content, headers):
    """Constructs a standard message with standard headers

    @param operation the operation (performative) of the message
    @param headers dict with headers that may override standard headers
    """
    msg = {}
    # The following headers are FIPA ACL Message Format based
    msg['sender'] = str(send)
    msg['receiver'] = str(recv)
    msg['reply-to'] = str(send)
    msg['encoding'] = 'json_1'
    msg['language'] = 'ion1'
    msg['format'] = 'raw'
    msg['ontology'] = ''
    msg['conv-id'] = ''
    msg['protocol'] = ''
    #msg['reply-with'] = ''
    #msg['in-reply-to'] = ''
    #msg['reply-by'] = ''
    msg.update(headers)
    msg['op'] = operation
    msg['content'] = content
    return receiver.send(recv, msg)

def dispatch_message(content, msg, dispatchIn):
    """
    content - content can be anything (list, tuple, dictionary, string, int, etc.)

    For this implementation, 'content' will be a dictionary:
    content = {
        "op": "operation name here",
        "content": ('arg1', 'arg2')
    }
    """
    try:
        log_message(__name__, content, msg)

        if "op" in content:
            op = content['op']
            logging.info('dispatch_message() OP=' + str(op))

            cont = content.get('content','')
            opname = 'op_' + str(op)

            # dynamically invoke the operation
            if hasattr(dispatchIn, opname):
                getattr(dispatchIn, opname)(cont, content, msg)
            elif hasattr(dispatchIn,'op_noop_catch'):
                dispatchIn.op_noop_catch(cont, content, msg)
            else:
                logging.error("Receive() failed. Cannot dispatch to catch")
        else:
            logging.error("Receive() failed. Bad message", content)
    except Exception, e:
        logging.error('Exception while dispatching: '+repr(e))
        (type, value, trace) = sys.exc_info()
        traceback.print_tb(trace)

 #       logging.error('Traceback: '+trace.format_exc())

id_seqs = {}
def create_unique_id(ns):
    """Creates a unique id for the given name space based on sequence counters.
    """
    if ns == None: ns = ':'
    nss = str(ns)
    if nss in id_seqs: nsc = int(id_seqs[nss]) +1
    else: nsc = 1
    id_seqs.put(nss, nsc)
    return nss + str(nsc)