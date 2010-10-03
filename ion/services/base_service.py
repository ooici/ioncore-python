#!/usr/bin/env python

"""
@file ion/services/base_service.py
@author Michael Meisinger
@brief base classes for all service processes and clients.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from ion.core.cc.container import Container
from ion.core.messaging.receiver import Receiver

from ion.core import base_process
from ion.core.base_process import BaseProcess, BaseProcessClient
import ion.util.procutils as pu

class BaseService(BaseProcess):
    """
    This is the superclass for all service processes.  A service process is a
    Capability Container process that can be spawned anywhere in the network
    and that provides a service under a defined service name (message queue).
    The service subclass must have declaration with defaule service name,
    version identifier and dependencies.
    """
    # Service declaration, to be set by the subclass
    declare = {}

    def __init__(self, receiver=None, spawnArgs=None):
        """
        Initializes base service. The default service name is taken from the
        service declaration, a different service name can be provided in the
        spawnargs using the 'servicename' attribute. The service name, in its
        qualified form prefixed by the system name is the public name of the
        service inbound queue that is shared among all service processes with
        the same name
        """
        BaseProcess.__init__(self, receiver, spawnArgs)

        # Determine public service messaging name either from spawn args or
        # use default name from service declaration
        #default_svcname = self.declare['name'] + '_' + self.declare['version']
        default_svcname = self.declare['name']
        self.svc_name = self.spawn_args.get('servicename', default_svcname)
        assert self.svc_name, "Service must have a declare with a valid name"

        # Scope (prefix) the service name with the system name
        msgName = self.get_scoped_name('system', self.svc_name)

        # Create a receiver (inbound queue consumer) for service name
        svcReceiver = Receiver(self.svc_name+'.'+self.receiver.label, msgName)
        if hasattr(self.receiver, 'group'):
            svcReceiver.group = self.receiver.group
        self.svc_receiver = svcReceiver
        self.svc_receiver.handle(self.receive)
        self.add_receiver(self.svc_receiver)

    @defer.inlineCallbacks
    def op_start(self, content, headers, msg):
        """
        Start service operation, on receive of a start message
        """
        yield defer.maybeDeferred(self.slc_start)
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_stop(self, content, headers, msg):
        """
        Stop service operation, on receive of a stop message
        """
        yield defer.maybeDeferred(self.slc_stop)
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def plc_init(self):
        yield self._declare_service_name()
        svcid = yield self.svc_receiver.activate()
        log.info('Service process bound to name=%s as pid=%s' % (self.svc_receiver.name, svcid))
        yield defer.maybeDeferred(self.slc_init)
        yield defer.maybeDeferred(self.slc_start)

    @defer.inlineCallbacks
    def _declare_service_name(self):
        # Ad hoc service exchange name declaration
        msgName = self.get_scoped_name('system', self.svc_name)
        messaging = {'name_type':'worker', 'args':{'scope':'system'}}
        yield Container.configure_messaging(msgName, messaging)

    @defer.inlineCallbacks
    def plc_shutdown(self):
        yield defer.maybeDeferred(self.slc_stop)
        yield defer.maybeDeferred(self.slc_shutdown)

    def slc_init(self):
        """
        Service life cycle event: initialization of service process. This is
        called once after the receipt of the process init message. Use this to
        perform complex, potentially deferred initializations.
        """
        #log.debug('slc_init()')

    def slc_start(self):
        """
        Service life cycle event: start of service process. Will be called once
        or many times after the slc_init of a service process. At this point,
        all service dependencies must be present.
        """
        #log.info('slc_start()')

    def slc_stop(self):
        """
        Service life cycle event: stop of service process. Will be called to
        stop a started service process, and before shutdown. A stopped service
        can be restarted again.
        """
        #log.info('slc_stop()')

    def slc_shutdown(self):
        """
        Service life cycle event: final shutdown of service process. Will be
        called after a slc_stop before the actual termination of the process.
        No further asyncronous activities are allowed by the process after
        reply from this function.
        """
        #log.info('slc_shutdown()')

    @classmethod
    def service_declare(cls, **kwargs):
        """
        Helper method to create a declaration of service.
        @param kwargs keyword attributes for service. Common ones must be present.
        @retval a dict with service attributes
        """
        log.debug("Service-declare: "+str(kwargs))
        decl = {}
        decl.update(kwargs)
        return decl

class BaseServiceClient(BaseProcessClient):
    """
    This is the base class for service client libraries. Service client libraries
    can be used from any process or standalone (in which case they spawn their
    own client process). A service client makes accessing the service easier and
    can perform client side optimizations (such as caching and transformation
    of certain service results).
    """
