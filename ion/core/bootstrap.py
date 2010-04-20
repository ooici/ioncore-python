#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@brief base class for bootstrapping the system
"""

import logging
from twisted.internet import defer
import time

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.util.config import Config
import ion.util.procutils as pu

logging.basicConfig(level=logging.DEBUG)

#logging.basicConfig(level=logging.DEBUG, \
#            format='%(asctime)s %(levelname)s (%(funcName)s) %(message)s')

# logging.info('Starting up...')

# Static definition of message queues
ion_queues = {}

# Static definition of service names
ion_services = Config('res/config/ionservices.cfg').getObject()


@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    yield store.put('bootstrap', id)
    yield op_bootstrap()

@defer.inlineCallbacks
def op_bootstrap():
    logging.info("Bootstrapping now...")
    yield bs_createservices()
    print "Store: ", store.kvs
   # yield bs_initmessage()

@defer.inlineCallbacks
def bs_createservices():
    logging.info("Importing service classes")
   
    for svc_name in ion_services:
        # logging.info('Adding ' + svc_name)
        print 'Adding ' + svc_name
        svc = ion_services[svc_name]
        
        # Importing service module
        print 'from ' + svc['module'] + " import " + svc['class']
        localmod = svc['module'].rpartition('.')[2]
        svc_mod = __import__(svc['module'], globals(), locals(), [localmod,svc['class']])
        svc['module_import'] = svc_mod
    
        # Spawn instance of a service
        svc_id = yield spawn(svc_mod)
        svc['instance'] = svc_id
        yield store.put(svc_name, svc_id)
        print "Service "+svc_name+" ID: ",svc_id        

@defer.inlineCallbacks
def bs_initmessage():
    logging.info("Sending init message to all services")
    
    for svc_name in ion_services:
        to = yield store.get(svc_name)
        print "Send to: ",to
        yield pu.send_message(receiver, '', to, 'init', {'name':svc_name}, {})

store = Store()
receiver = Receiver(__name__)

def receive(content, msg):
    pu.log_message(__name__, content, msg)

receiver.handle(receive)

