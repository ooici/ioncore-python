#!/usr/bin/env python

"""
@file ion/core/cc/cc_agent.py
@author Michael Meisinger
@brief capability container control process
"""

import logging

from magnet.spawnable import Receiver

from ion.core.supervisor import Supervisor, ChildProcess
from ion.core.base_process import ProtocolFactory
from ion.agents.resource_agent import ResourceAgent


class CCAgent(ResourceAgent):
    """Capability Container agent process interface
    """

    def plc_init(self):
        self.supervisor = Supervisor(self.receiver)

    def op_start_node(self, content, headers, msg):
        pass

    def op_terminate_node(self, content, headers, msg):
        pass

    def op_spawn(self, content, headers, msg):
        procMod = content['module']
        child = ChildProcess(procMod)
        pass

    def op_get_node_id(self, content, headers, msg):
        pass

    def op_advertise(self, content, headers, msg):
        pass

    def op_get_config(self, content, headers, msg):
        pass

# Spawn of the process using the module name
factory = ProtocolFactory(CCAgent)
