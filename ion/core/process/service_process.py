#!/usr/bin/env python

"""
@file ion/core/process/service_process.py
@author Michael Meisinger
@brief base classes for all service processes and clients.
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.process.process import Process, ProcessClient, ProcessFactory
from ion.core.cc.container import Container
from ion.core.messaging.receiver import ServiceWorkerReceiver
import ion.util.procutils as pu

class IServiceProcess(Interface):
    """
    Interface for all capability container service worker processes
    """

class ServiceProcess(Process):
    """
    This is the superclass for all service processes.  A service process is a
    Capability Container process that can be spawned anywhere in the network
    and that provides a service under a defined service name (message queue).
    The service subclass must have declaration with defaule service name,
    version identifier and dependencies.
    """
    implements(IServiceProcess)

    # Service declaration, to be set by the subclass
    declare = {}

    def __init__(self, *args, **kwargs):
        """
        Initializes base service. The default service name is taken from the
        service declaration, a different service name can be provided in the
        spawnargs using the 'servicename' attribute. The service name, in its
        qualified form prefixed by the system name is the public name of the
        service inbound queue that is shared among all service processes with
        the same name
        """
        Process.__init__(self, *args, **kwargs)

        # Determine public service messaging name either from spawn args or
        # use default name from service declaration
        #default_svcname = self.declare['name'] + '_' + self.declare['version']
        default_svcname = self.declare['name']
        self.svc_name = self.spawn_args.get('servicename', default_svcname)
        assert self.svc_name, "Service must have a declare with a valid name"

        # Create a receiver (inbound queue consumer) for service name
        self.svc_receiver = ServiceWorkerReceiver(
                label=self.svc_name+'.'+self.receiver.label,
                name=self.svc_name,
                scope='system',
                group=self.receiver.group,
                process=self, # David added this - is it a good idea?
                handler=self.receive)
        self.add_receiver(self.svc_receiver)

    @defer.inlineCallbacks
    def plc_init(self):
        # Step 1: Service init callback
        try:
            yield defer.maybeDeferred(self.slc_init)
        except Exception, ex:
            log.exception('----- Service %s process %s INIT ERROR -----' % (self.svc_name, self.id))
            raise ex

        # Step 2: Init service name receiver (declare queue)
        yield self.svc_receiver.initialize()

    def slc_init(self):
        """
        Service life cycle event: initialization of service process. This is
        called once after the receipt of the process init message. Use this to
        perform complex, potentially deferred initializations.
        """
        #log.debug('slc_init()')

    @defer.inlineCallbacks
    def plc_activate(self):
        # Step 1: Service activate callback
        try:
            yield defer.maybeDeferred(self.slc_activate)
        except Exception, ex:
            log.exception('----- Service %s process %s ACTIVATE ERROR -----' % (self.svc_name, self.id))
            raise ex

        # Step 2: Activate service name receiver (activate consumer)
        yield self.svc_receiver.activate()
        log.info('Service process bound to name=%s' % (self.svc_receiver.xname))

    def slc_activate(self):
        """
        Service life cycle event: activate service process. Will be called once
        or many times after the slc_init of a service process. At this point,
        all service dependencies must be present.
        """
        #log.info('slc_start()')

    @defer.inlineCallbacks
    def plc_deactivate(self):
        # Step 1: Activate service name receiver (deactivate consumer)
        yield self.svc_receiver.deactivate()
        log.info('Service process detached from name=%s' % (self.svc_receiver.xname))

        # Step 2: Service deactivate callback
        try:
            yield defer.maybeDeferred(self.slc_deactivate)
        except Exception, ex:
            log.exception('----- Service %s process %s DEACTIVATE ERROR -----' % (self.svc_name, self.id))
            raise ex

    def slc_deactivate(self):
        """
        Service life cycle event: deactivate service process. Will be called to
        deactivate an activated service process, and before terminate. A
        deactivated service can be restarted again.
        """
        #log.info('slc_deactivate()')

    @defer.inlineCallbacks
    def plc_terminate(self):
        try:
            yield defer.maybeDeferred(self.slc_deactivate)
            yield defer.maybeDeferred(self.slc_terminate)
        except Exception, ex:
            log.exception('----- Service %s process %s TERMINATE ERROR -----' % (self.svc_name, self.id))
            raise ex

        # These appear to be left over from a previous verison of the code
        #yield defer.maybeDeferred(self.slc_stop)
        #yield defer.maybeDeferred(self.slc_shutdown)

    def slc_terminate(self):
        """
        Service life cycle event: final shutdown of service process. Will be
        called after a slc_deactivate before the actual termination of the process.
        No further asyncronous activities are allowed by the process after
        reply from this function.
        """
        #log.info('slc_terminate()')

    @classmethod
    def service_declare(cls, **kwargs):
        """
        Helper method to create a declaration of service.
        @param kwargs keyword attributes for service. Common ones must be present.
        @retval a dict with service attributes
        """
        log.debug("Service-declare: %s" % (kwargs))
        return kwargs

factory = ProcessFactory(ServiceProcess)

class ServiceClient(ProcessClient):
    """
    This is the base class for service client libraries. Service client libraries
    can be used from any process or standalone (in which case they spawn their
    own client process). A service client makes accessing the service easier and
    can perform client side optimizations (such as caching and transformation
    of certain service results).
    """
