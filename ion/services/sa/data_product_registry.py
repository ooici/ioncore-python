#!/usr/bin/env python

"""
@file ion/services/sa/data_product_registry.py
@author Michael Meisinger
@brief service for registering data products
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class DataProductRegistryService(BaseService):
    """Data product registry service interface. Data products are for instance
    created as a pipeline of refining raw data through automatic and manual
    QAQC processes.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_product_registry', version='0.1.0', dependencies=[])
 
    def op_define_data_product(self, content, headers, msg):
        """Service operation: Create or update a data product definition.
        """

    def op_find_data_product(self, content, headers, msg):
        """Service operation: Find a data product definition by characteristics.
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(DataProductRegistryService)
