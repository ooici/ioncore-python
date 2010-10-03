#!/usr/bin/env python

"""
@file ion/services/base_svcproc.py
@author Michael Meisinger
@brief base class for service processes within Magnet
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core.messaging.receiver import Receiver
from ion.core.base_process import BaseProcess, ProcessFactory
import ion.util.procutils as pu

class BaseServiceProcess(BaseProcess):
    """
    This is a base class for service processes.

    A service process is a Capability Container process that can be spawned
    anywhere in the network and that provides a service. This process actually
    instantiates the service class.
    """

class ProcessProcessFactory(ProcessFactory):
    """This protocol factory actually returns a receiver for a new service
    process instance, as named in the spawn args.
    """
    def build(self, spawnArgs={}):
        """Factory method return a new receiver for a new process. At the same
        time instantiate class.
        """
        log.info("ProcessProcessFactory.build() with args="+str(spawnArgs))
        svcmodule = spawnArgs.get('svcmodule',None)
        if not svcmodule:
            log.error("No spawn argument svcmodule given. Cannot spawn")
            return None

        svcclass = spawnArgs.get('svcclass',None)

        svc_mod = pu.get_module(svcmodule)

        if hasattr(svc_mod,'factory'):
            log.info("Found module factory. Using factory to get service receiver")
            return svc_mod.factory.build()
        elif hasattr(svc_mod,'receiver'):
            log.info("Found module receiver")
            return svc_mod.receiver
        elif svcclass:
            log.info("Service process module instantiate from class:"+svcclass)
            return self.create_process_instance(svc_mod,'name')
        else:
            log.error("Service process module cannot be spawned")

    def create_process_instance(self, svc_mod, className):
        """Given a class name and a loaded module, instantiate the class
        with a receiver.
        """
        svc_class = pu.get_class(className, svc_mod)
        #if not issubclass(svc_class,BaseProcess):
        #    raise RuntimeError("class is not a BaseProcess")

        receiver = Receiver(svc_mod.__name__)
        serviceInstance = svc_class(receiver)
        log.info('create_process_instance: created service instance '+str(serviceInstance))
        return receiver

# Spawn of the process using the module name
factory = ProcessProcessFactory()

"""
from ion.services import base_svcproc as b
spawn(b,None,{'svcmodule':'ion.services.hello_service'})
send(1, {'op':'hello','content':'Hello you there!'})
"""
