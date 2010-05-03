#!/usr/bin/env python

"""
@file ion/services/dm/dataset_registry.py
@author Michael Meisinger
@brief service for registering datasets
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class DatasetRegistryService(BaseService):
    """Dataset registry service interface
    """
 
    def op_define_dataset(self, content, headers, msg):
        """Service operation: Create or update a dataset resource.
        """

    def op_find_dataset(self, content, headers, msg):
        """Service operation: Find dataset resource by characteristics
        """

    def op_get_dataset(self, content, headers, msg):
        """Service operation: Get dataset description
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(DatasetRegistryService)
