#!/usr/bin/env python

"""
@file ion/services/coi/identity_registry.py
@author Michael Meisinger
@brief service for registering and authenticating identities
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

class IdentityRegistryService(BaseService):
    """(User and resource) identity registry service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='identity_registry', version='0.1.0', dependencies=[])

    def op_define_identity(self, content, headers, msg):
        """Service operation: .
        """

    def op_register_user(self, content, headers, msg):
        """Service operation: .
        """

    def op_define_user_profile(self, content, headers, msg):
        """Service operation: .
        """

    def op_authenticate(self, content, headers, msg):
        """Service operation: .
        """

# Spawn of the process using the module name
factory = ProtocolFactory(IdentityRegistryService)

