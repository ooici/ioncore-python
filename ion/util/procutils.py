#!/usr/bin/env python

"""
@file ion/util/procutils.py
@author Michael Meisinger
@package ion.util  utility helper functions for processes in capability containers
"""

from twisted.python import log
from twisted.internet import defer

# Build a service properties dict
def service_deploy_factory(package,name):
    scv_dict = {'name' : name,
                'package' : package,
                'module' : name,
                }
    return scv_dict

# Print an object's attributes
def print_attributes(obj):
    str = ''
    for attr, value in obj.__dict__.iteritems():
        #str = str + attr + ": " + value + "\n"
        print attr, ": ", value
    #print str

def log_message(proc,content,msg):
    print "===Message=== @", proc
    print_attributes(msg)
    print "-------------"
    print content    
    print "============="

# Returns the instance part of a long process id 
def get_process_id(long_id):
    parts = str(long_id).rpartition('.')
    return parts[2]

# Constructs a message with standard headers    
def send_message(receiver,src,to,operation,content,headers):
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
