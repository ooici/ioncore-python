#!/usr/bin/env python

"""
@file ion/services/cei/provisioner.py
@author Michael Meisinger
@author Alex Clemesha
@brief Starts, stops, and tracks instance and context state.
"""

import logging
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory

class ProvisionerService(BaseService):
    """Provisioner service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='provisioner', version='0.1.0', dependencies=[])

    def op_provision(self, content, headers, msg):
        """Service operation: Provision a taskable resource
        """

    def op_terminate(self, content, headers, msg):
        """Service operation: Terminate a taskable resource
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(ProvisionerService)
