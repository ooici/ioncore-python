#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@package ion.core bootstrapping the system.
"""

from twisted.python import log
from twisted.internet import defer
import logging
import time

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu
from ion.util.procutils import service_deploy_factory

# Static definition of service names and message queues
# @todo

# Static definition of services and properties
ion_services = {
        'datastore' :service_deploy_factory ('ion.services.coi','datastore'),
        'resource_registry' :service_deploy_factory ('ion.services.coi','resource_registry'),
        'service_registry' :service_deploy_factory ('ion.services.coi','service_registry'),
        'exchange_registry' :service_deploy_factory ('ion.services.coi','exchange_registry'),
        'provisioner' :service_deploy_factory ('ion.services.cei','provisioner'),
        'dataset_registry' :service_deploy_factory ('ion.services.dm','dataset_registry'),
        }

logging.basicConfig(level=logging.DEBUG)

#logging.basicConfig(level=logging.DEBUG, \
#            format='%(asctime)s %(levelname)s (%(funcName)s) %(message)s')

# logging.info('Starting up...')

store = Store()

receiver = Receiver(__name__)


@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('bootstrap', id)
    yield op_bootstrap()

@defer.inlineCallbacks
def op_bootstrap():
    print "Bootstrapping now"
    
    for svc_name in ion_services:
        # logging.info('Adding ' + svc_name)
        print 'Adding ' + svc_name
        svc = ion_services[svc_name]
        
        # Importing service module
        svc_import = svc['package'] + "." + svc['module']
        print 'Import ' + svc_import
        svc_mod = __import__(svc_import, globals(), locals(), [svc['module']])
        svc['module_import'] = svc_mod
    
        # Spawn instance of a service
        svc_id = yield spawn(svc_mod)
        svc['instance'] = svc_id
        yield store.put(svc['name'], svc_id)
        print "Service "+svc['name']+" ID: ",svc_id
        
        # Send a start message to service instance
#        to = yield store.get(svc['name'])
#        print "Send to: ",to
#        receiver.send(to, {'op':'START','args':{}})

    print "Store: ", store.kvs

def receive(content, msg):
    pu.log_message(__name__, content, msg)

receiver.handle(receive)

