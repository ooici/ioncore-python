#!/usr/bin/env python

"""
@file ion/services/sa/data_acquisition.py
@author Michael Meisinger
@brief service for data acquisition
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class DataAcquisitionService(BaseService):
    """Data acquisition service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_acquisition', version='0.1.0', dependencies=[])
 
    def op_acquire_block(self, content, headers, msg):
        """Service operation: Acquire an entire, fully described version of a
        data set.
        """

    def op_acquire_message(self, content, headers, msg):
        """Service operation: Acquire an increment of a data set.
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(DataAcquisitionService)
