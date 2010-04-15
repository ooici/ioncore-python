#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@package ion.misc bootstrapping the system.
"""

from twisted.python import log
from twisted.internet import defer
import logging

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

# Build a service properties dict
def service_deploy_factory(package,name):
    scv_dict = {'name' : name,
    			'package' : package,
    			'module' : name,
    			}
    return scv_dict

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

logging.info('Starting up...')

store = Store()

receiver = Receiver(__name__)
started = False

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('bootstrap', id)
    
@defer.inlineCallbacks
def op_bootstrap():
    print "Bootstrapping now"
    started = True
    
    for svc_name in ion_services:
        # logging.info('Adding ' + svc_name)
        print('Adding ' + svc_name)
        svc = ion_services[svc_name]
        
        # Importing service module
        svc_import = svc['package'] + "." + svc['module']
        print('Import ' + svc_import)
        svc_mod = __import__(svc_import, globals(), locals(), [svc['module']])
        svc['module_import'] = svc_mod
    
    	# Spawn instance of a service
        svc_id = yield spawn(svc_mod)
        store.put(svc['name'], svc_id)
        print("Service "+svc['name']+" ID: ",svc_id)
        
        # Send a start message to service instance
        to = yield store.get(svc['name'])
        print("Send to: ",to)
        receiver.send(to, {'method':'START','args':{}})

    
#    to = yield store.get('datastore')
#    receiver.send(to, {'method':'PUT','args':{'key':'obj1','value':999}})

#    replyto = yield store.get('bootstrap')
#    receiver.send(to, {'method':'GET','args':{'key':'obj1','reply-to':replyto}})

def receive(content, msg):

    print 'in receive ', content
    if not started:
        op_bootstrap()

receiver.handle(receive)
