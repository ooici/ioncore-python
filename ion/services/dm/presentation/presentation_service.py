#!/usr/bin/env python

"""
@file ion/services/dm/presentation/presentation_service.py
@author David Stuebe
@brief service for presentation of DM resources and services.
"""



import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class PresentationService(BaseService):
    """Presentation service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='presentation', version='0.1.0', dependencies=[])
 
    def op_present_catalog(self, content, headers, msg):
        """Service operation: TBD
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(PresentationService)
