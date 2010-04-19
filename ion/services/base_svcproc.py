#!/usr/bin/env python

"""
@file ion/services/base_svcproc.py
@author Michael Meisinger
@brief base class for all service processes within Magnet
"""

import logging

from twisted.internet import defer

from ion.core.base_process import BaseProcess

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class BaseServiceProcess(BaseProcess):
    """
    This is the base class for all service processes.
    
    A service process is a Capability Container process that can be spawned
    anywhere in the network and that provides a service.
    """
    
    # Name of the spawnable of the executed process
    processName = None
    
    # Fully qualified name of the service module
    serviceModule = None
    
    # Name of the service class within service module
    serviceName = None
    
    # An instance of the service class
    serviceInstance = None
    
    def __init__(self, procName, svcMod, svcName):
        """Constructor.
        @param procName public name of the spawnable process
        @param svcMod qualified name of the module in which the service class
            is, e.g. 'ion.services.coi.resource_registry'
        @param svcName name of the service class, e.g. 'ResourceRegistryService'
        """
        logging.info('BaseServiceProcess.__init__('+procName+','+svcMod+','+svcName+')')
        BaseProcess.__init__(self, procName)

        self.processName = procName
        self.serviceModule = svcMod
        self.serviceName = svcName
        
        localMod = svcMod.rpartition('.')[2]
        logging.info('BaseServiceProcess.__init__: from '+self.serviceModule+' import '+localMod+', '+svcName)
        
        svc_mod = __import__(self.serviceModule, globals(), locals(), [localMod,svcName])
        #logging.debug('Module: '+str(svc_mod))

        svc_class = getattr(svc_mod, svcName)
        #logging.debug('Class: '+str(svc_class))
        
        self.serviceInstance = svc_class()
        self.serviceInstance.receiver = self.receiver
        logging.info('BaseServiceProcess.__init__: created service instance '+str(self.serviceInstance) )


    def receive(self, content, msg):
        logging.info('BaseServiceProcess.receive()')
        self._dispatch_message(content, msg, self.serviceInstance)

# Code below is for starting this base class directly as a process

@defer.inlineCallbacks
def start(svcMod, svcName):
    """Starts a new process and tries to instantiate the given service class
    in the given module.
    
    @param svcMod qualified name of the module in which the service class is,
        e.g. 'ion.services.coi.resource_registry'
    @param svcName name of the service class, e.g. 'ResourceRegistryService'
    """
    
    logging.info('BaseServiceProcess.start: '+svcMod+':'+svcName+' in proc '+__name__)
    procInst = BaseServiceProcess(__name__, svcMod, svcName)
    procInst.receiver.handle(procInst.receive)

    yield procInst.plc_start()
    #logging.debug('procInst: '+str(procInst.__dict__))

    
"""
from ion.services import base_svcproc as b
b.start('ion.services.hello_service','HelloService')
send(1, {'op':'hello','content':'Hello you there!'})

"""