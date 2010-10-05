#!/usr/bin/env python

"""
@author Dorian Raymer
@author Michael Meisinger
@brief Capability Container main class
@see http://www.oceanobservatories.org/spaces/display/syseng/CIAD+COI+SV+Python+Capability+Container

A container utilizes the messaging abstractions for AMQP.

"""

import os
import sys

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.cc.container_api import IContainer
from ion.core.id import Id
from ion.core.messaging.exchange import ExchangeManager
from ion.core.pack.application import AppLoader
from ion.core.pack.app_manager import AppManager
from ion.core.process.proc_manager import ProcessManager
from ion.util.state_object import BasicLifecycleObject

CONF = ioninit.config(__name__)

class Container(BasicLifecycleObject):
    """
    Represents an instance of the Capability Container. Typically, in one Twisted
    process (= one UNIX process), there is only one instance of a CC. In test cases,
    however, there might be more.
    """
    implements(IContainer)

    # Static variables
    id = '%s.%d' % (os.uname()[1], os.getpid())
    args = None  # Startup arguments
    _started = False

    def __init__(self):
        BasicLifecycleObject.__init__(self)

        # Config instance
        self.config = None

        # ExchangeManager instance
        self.exchange_manager = None

        # ProcessManager instance
        self.proc_manager = None

        # AppManager instance
        self.app_manager = None

    def on_initialize(self, config, *args, **kwargs):
        """
        Initializes the instance of a container. Actions include
        - Receive and parse the configuration
        - Prepare some active objects
        """
        self.config = config

        # Set additional container args
        Container.args = self.config.get('args', None)

        self.exchange_manager = ExchangeManager(self)
        self.exchange_manager.initialize(config, *args, **kwargs)

        self.proc_manager = ProcessManager(self)
        self.proc_manager.initialize(config, *args, **kwargs)

        self.app_manager = AppManager(self)
        self.app_manager.initialize(config, *args, **kwargs)

        return defer.succeed(None)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        Activates the container. Actions include
        - Initiate broker connection
        - Start
        @retval Deferred
        """
        Container._started = True

        yield self.exchange_manager.activate()

        yield self.proc_manager.activate()

        yield self.app_manager.activate()

    def on_deactivate(self, *args, **kwargs):
        raise NotImplementedError("Not implemented")

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        Deactivates and terminates the container. Actions include
        - Stop and terminate all container applications
        - Close broker connection
        @retval Deferred
        """

        yield self.app_manager.terminate()

        yield self.proc_manager.terminate()

        yield self.exchange_manager.terminate()

        log.info("Container closed")
        Container._started = False

    def on_error(self, *args, **kwargs):
        raise RuntimeError("Illegal state change for container")

    # --- Container API -----------

    # Process management, handled by ProcessManager
    def spawn_process(self, *args, **kwargs):
        return self.proc_manager.spawn_process(*args, **kwargs)
    def spawn_processes(self, *args, **kwargs):
        return self.proc_manager.spawn_processes(*args, **kwargs)
    def create_supervisor(self, *args, **kwargs):
        return self.proc_manager.create_supervisor(*args, **kwargs)
    def activate_process(self, *args, **kwargs):
        return self.proc_manager.activate_process(*args, **kwargs)
    def terminate_process(self, *args, **kwargs):
        return self.proc_manager.terminate_process(*args, **kwargs)

    # Exchange management, handled by ExchangeManager
    def declare_messaging(self, *args, **kwargs):
        return self.exchange_manager.declare_messaging(*args, **kwargs)
    def configure_messaging(self, *args, **kwargs):
        return self.exchange_manager.configure_messaging(*args, **kwargs)
    def new_consumer(self, *args, **kwargs):
        return self.exchange_manager.new_consumer(*args, **kwargs)
    def send(self, *args, **kwargs):
        return self.exchange_manager.send(*args, **kwargs)

    # App management, handled by AppManager
    def start_app(self, *args, **kwargs):
        return self.app_manager.start_app(*args, **kwargs)

    def start_rel(self, rel_filename):
        pass

    def __str__(self):
        return "CapabilityContainer(state=%s,%r)" % (
            self._get_state(),
            self.exchange_manager.message_space)

def create_new_container():
    """
    Factory for a container.
    This also makes sure that only one container is active at any time,
    currently.
    """
    if Container._started:
        raise RuntimeError('Already started')

    c = Container()
    ioninit.container_instance = c

    return c

Id.default_container_id = Container.id
