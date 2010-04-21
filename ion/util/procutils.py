#!/usr/bin/env python

"""
@file ion/util/procutils.py
@author Michael Meisinger
@brief  utility helper functions for processes in capability containers
"""

import logging
from twisted.python import log
from twisted.internet import defer

from magnet.container import Id

logging.basicConfig(level=logging.DEBUG)

def log_attributes(obj):
    """Print an object's attributes
    """
    lstr = ""
    for attr, value in obj.__dict__.iteritems():
        if attr != 'content':
            lstr = lstr + str(attr) + ": " +str(value) + ", "
    logging.info(lstr)

def log_message(proc,content,msg):
    """Log an incoming message with all headers
    """
    logging.info("===Message=== @" + str(proc))
    log_attributes(msg)
    logging.info("-------------")
    logging.info(content)
    logging.info("=============")

def get_process_id(long_id):
    """Returns the instance part of a long process id 
    """
    parts = str(long_id).rpartition('.')
    if parts[1] != '':
        procId = Id(parts[2],parts[0])
    else:
        procId = Id(long_id)
    return procId
   
def send_message(receiver,src,to,operation,content,headers):
    """Constructs a message with standard headers
    """
    msg = {}
    msg.update(headers)
    msg['op'] = operation
    msg['sender'] = str(src)
    msg['receiver'] = str(to)
    msg['encoding'] = ''
    msg['structure'] = ''
    msg['semantics'] = ''
    msg['conversation'] = ''
    msg['intpattern'] = ''
    msg['content'] = content
    receiver.send(to,msg)
    
def dispatch_message(content, msg, dispatchIn):
    """
    content - content can be anything (list, tuple, dictionary, string, int, etc.)

    For this implementation, 'content' will be a dictionary:
    content = {
        "op": "operation name here",
        "content": ('arg1', 'arg2')
    }
    """
    
    log_message(__name__, content, msg)
    
    if "op" in content:
        op = content['op']            
        logging.info('dispatch_message() OP='+op)

        cont = content.get('content','')

        # dynamically invoke the operation
        if hasattr(dispatchIn,'op_' + op):
            getattr(dispatchIn, 'op_' + op)(cont, content, msg)
        elif hasattr(dispatchIn,'op_noop_catch'):
            dispatchIn.op_noop_catch(cont, content, msg)
        else:
            logging.error("Receive() failed. Cannot dispatch to catch")
    else:
        logging.error("Receive() failed. Bad message", content)
