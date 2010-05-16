#!/usr/bin/env python

"""
@file ion/services/dm/transform.py
@author Michael Meisinger
@brief service for transforming information
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class TransformService(BaseService):
    """Transformation service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='transform', version='0.1.0', dependencies=[])
 
    def op_transform(self, content, headers, msg):
        """Service operation: TBD
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(TransformService)
