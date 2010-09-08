#!/usr/bin/env python

"""
@file ion/services/cei/comp_planner.py
@author Michael Meisinger
@brief service for requesting and planning computations
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class ComputationPlannerService(BaseService):
    """Provisioner service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='computation_planner', version='0.1.0', dependencies=[])

    def op_request_computation(self, content, headers, msg):
        """Service operation: Request computation resources 
        """

    def op_schedule_computation(self, content, headers, msg):
        """Service operation: Request computation with given schedule
        """

    def op_set_policy(self, content, headers, msg):
        """Service operation: Sets the policy that influences the planning of
        computation resource scheduling and provisioning.
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(ComputationPlannerService)
