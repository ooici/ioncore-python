#!/usr/bin/env python

"""
@file ion/core/supervisor.py
@author Michael Meisinger
@brief base class for processes that supervise other processes
"""

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

class Supervisor(BaseProcess):

    # Static definition of services and properties
    dependent_services = {}

    def spawnDependents(self):
        for svc_name in dependent_services:
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

    def event_failure(self):
        return
    
    