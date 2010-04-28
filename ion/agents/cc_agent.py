#!/usr/bin/env python

"""
@file ion/agents/cc_agent.py
@author Michael Meisinger
@brief capability container control
"""

import logging

from magnet.spawnable import Receiver

from ion.core.supervisor import Supervisor, ChildProcess
from ion.services.base_service import BaseService


logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class CCAgent(BaseService):
    """Capability Container agent service interface
    """

    def slc_init(self):
        self.supervisor = Supervisor(self.receiver)

    def op_spawn(self, content, headers, msg):
        procMod = content['module']
        child = ChildProcess(procMod)
        pass

    def op_getNodeId(self, content, headers, msg):
        pass

    def op_advertise(self, content, headers, msg):
        pass

    def op_getConfig(self, content, headers, msg):
        pass


# Direct start of the service as a process with its default name
receiver = Receiver(__name__)
instance = CCAgent(receiver)
