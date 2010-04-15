#!/usr/bin/env python

"""
@file ion/core/bootstrap.py
@author Michael Meisinger
@package ion.misc bootstrapping the system.
"""

from twisted.python import log
from twisted.internet import defer
#import logging

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.services.coi import datastore

def service_deploy_factory(package,name):
    scv_dict = {'name' : name,
    			'package' : package,
    			'module' : name,
    			}
    return scv_dict

ion_services = {
        'datastore' :service_deploy_factory ('ion.services.coi','datastore'),
        'resource_registry' :service_deploy_factory ('ion.services.coi','resource_registry'),
        'service_registry' :service_deploy_factory ('ion.services.coi','service_registry'),
        'exchange_registry' :service_deploy_factory ('ion.services.coi','exchange_registry'),
        'provisioner' :service_deploy_factory ('ion.services.cei','provisioner'),
        'dataset_registry' :service_deploy_factory ('ion.services.dm','dataset_registry'),
        }

#logging.basicConfig(level=logging.DEBUG, \
#            format='%(asctime)s %(levelname)s (%(funcName)s) %(message)s')

#logging.info('Starting up...')

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
        
        execcmd = "from " + svc['package'] + " import " + svc['module']
        print('Eval ' + execcmd)
      #  logging.info('Eval ' + execcmd)
        exec(execcmd)
        
        svcid = yield spawn(svc['module'])
        store.put(svc['name'], id)
        
#        to = yield store.get(svc['name'])
#        replyto = yield store.get('bootstrap')
#        receiver.send(to, {'method':'START','args':{'supervisor':''+replyto}})

    
#    to = yield store.get('datastore')
#    receiver.send(to, {'method':'PUT','args':{'key':'obj1','value':999}})

#    replyto = yield store.get('bootstrap')
#    receiver.send(to, {'method':'GET','args':{'key':'obj1','reply-to':replyto}})

def receive(content, msg):

    print 'in receive ', content
    if not started:
        op_bootstrap()

receiver.handle(receive)
