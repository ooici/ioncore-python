#!/usr/bin/env python

"""
@file ion/agents/cc_agent.py
@author Michael Meisinger
@brief capability container control
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.services.base_service import BaseService, BaseServiceClient, RpcClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class CCAgent(BaseService):
    """Capability Container agent service interface
    """

    def op_spawn(self, content, headers, msg):
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
