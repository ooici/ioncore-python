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
from ion.core.process.process import Process, ProcessClient
from ion.core.cc.container import Container
from ion.core.messaging.receiver import ServiceWorkerReceiver
import ion.util.procutils as pu

class IAgentProcess(Interface):
    """
    Interface for all capability container agent processes
    """

class AgentProcess(Process):
    """
    This is the superclass for all agent processes.
    """
    implements(IAgentProcess)
