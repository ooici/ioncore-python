#!/usr/bin/env python

from twisted.internet import defer

from magnet.spawnable import spawn

from ion.agents.resource_agent import ResourceAgent
from ion.core.base_process import BaseProcessClient
from ion.core.base_process import ProtocolFactory


class InstrumentAgent(ResourceAgent):
    """
    The base class for developing Instrument Agents. This defines
    the interface to use for an instrumen agent.
    """

    def op_get(self, content, headers, msg):
        """
        """

    def op_set(self, content, headers, msg):
        """
        """

    def op_getLifecycleState(self, content, headers, msg):
        """
        """

    def op_setLifecycleState(self, content, headers, msg):
        """
        """

    def op_execute(self, content, headers, msg):
        """
        """

    def op_getStatus(self, content, headers, msg):
        """
        """

    def op_getCapabilities(self, content, headers, msg):
        """
        """

class InstrumentAgentClient(BaseProcessClient):
    """
    The base class for an Instrument Agent Client. It is a service
    that allows for RPC messaging
    """
