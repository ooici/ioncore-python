#!/usr/bin/env python

"""
@file ion/core/process/cprocess.py
@author Michael Meisinger
@brief base classes for internal processes within a capability container
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject

class IInvocation(Interface):
    pass

class Invocation(object):
    """
    Container object for parameters of events/messages passed to internal
    capability container processes
    """
    implements(IInvocation)

    # Event inbound processing path
    PATH_IN = 'in'

    # Event outbound processing path
    PATH_OUT = 'out'

    # Event processing path
    PATH_ANY = 'any'

    # Event processing should proceed
    STATUS_PROCESS = 'process'

    # Event processing is completed.
    STATUS_DONE = 'done'

    # Event processing should stop and event dropped with no action
    STATUS_DROP = 'drop'

    # Event processing should proceed with lower priority process, if any
    STATUS_REJECT = 'reject'

    # An error has occurred and event processing should stop
    STATUS_ERROR = 'error'

    def __init__(self, **kwargs):
        """
        @param path A path designator str, e.g. a constant or other
        @param message the message envelope
        @param content the message content
        @param status the processing status
        @param route None or a str indicating subsequent routing
        """
        self.args = kwargs
        self.path = str(kwargs.get('path', Invocation.PATH_ANY))
        self.message = kwargs.get('message', None)
        self.content = kwargs.get('content', None)
        self.status = kwargs.get('status', Invocation.STATUS_PROCESS)
        self.route = str(kwargs.get('route', ""))
        self.workbench = kwargs.get('workbench',None)
        self.note = None

    def drop(self, note=None):
        self.note = note
        self.status = Invocation.STATUS_DROP

    def done(self, note=None):
        self.note = note
        self.status = Invocation.STATUS_DONE

    def error(self, note=None):
        self.note = note
        self.status = Invocation.STATUS_ERROR

    def proceed(self, route=""):
        self.status = Invocation.STATUS_PROCESS
        self.route = str(route)

class IContainerProcess(Interface):
    """
    Interface for all capability container internal processes
    """
    def process(invocation):
        """
        @param invocation container object for parameters
        @retval invocation instance, may be modified
        """

class ContainerProcess(BasicLifecycleObject):
    """
    Base class for capability container internal processes.
    """
    implements(IContainerProcess)

    def __init__(self, name):
        BasicLifecycleObject.__init__(self)
        self.name = name

    # Life cycle

    def on_initialize(self, *args, **kwargs):
        """
        """
        return defer.succeed(None)

    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        return defer.succeed(None)

    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        return defer.succeed(None)

    def on_error(self, cause= None, *args, **kwargs):
        if cause:
            log.error("Process error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal container process state change")

    # Interface API

    def process(self, invocation):
        pass

    # Helpers
