#!/usr/bin/env python

"""
@file ion/util/procutils.py
@author Michael Meisinger
@brief  utility helper functions for processes in capability containers
"""

import logging
from twisted.python import log
from twisted.internet import defer

logging.basicConfig(level=logging.DEBUG)

def print_attributes(obj):
    """Print an object's attributes
    """
    for attr, value in obj.__dict__.iteritems():
        logging.info(str(attr)+": "+str(value))

def log_message(proc,content,msg):
    """Log an incoming message with all headers
    """
    logging.info("===Message=== @" + str(proc))
    print_attributes(msg)
    logging.info("-------------")
    logging.info(content)
    logging.info("=============")

def get_process_id(long_id):
    """Returns the instance part of a long process id 
    """
    parts = str(long_id).rpartition('.')
    return parts[2]
   
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

        # TODO: Null error check 'cont'
        cont = content['content']

        # dynamically invoke the operation
        opdef = getattr(dispatchIn, 'op_' + op)
        if opdef != None:
            opdef(cont, content, msg)
        elif getattr(dispatchIn,'op_noop_catch') == None:
            logging.error("Receive() failed. Cannot dispatch to catch")
        else:
            dispatchIn.op_noop_catch(cont, content, msg)
    else:
        logging.error("Receive() failed. Bad message", content)