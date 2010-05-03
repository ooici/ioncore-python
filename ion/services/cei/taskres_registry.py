#!/usr/bin/env python

"""
@file ion/services/cei/taskres_registry.py
@author Michael Meisinger
@brief service for storing and accessing taskable resource definitions
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class TaskableResourceRegistryService(BaseService):
    """Taskable resource registry and definition repository service interface
    """

    def op_define_resource(self, content, headers, msg):
        """Service operation: Create or update taskable resource description
        """

    def op_find_resource(self, content, headers, msg):
        """Service operation: Create or update taskable resource description
        """

    def op_store_resource(self, content, headers, msg):
        """Service operation: Store the definition of a taskable resource, e.g.
        source code, virtual machine image (or a pointer to it)
        """

    def op_retrieve_resource(self, content, headers, msg):
        """Service operation: Retrieve the definition of a taskable resource
        """        
        
# Spawn of the process using the module name
factory = ProtocolFactory(TaskableResourceRegistryService)
