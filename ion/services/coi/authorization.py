#!/usr/bin/env python

"""
@file ion/services/coi/authorization.py
@author Michael Meisinger
@brief service authorizing access and managing policy within an Org
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.services.base_service import BaseService, BaseServiceClient

class AuthorizationService(BaseService):
    """Authorization service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='authorization', version='0.1.0', dependencies=[])

    def op_authorize(self, content, headers, msg):
        """Service operation:
        """


    """
    Begin experimental methods RU create backend module.py files if needed. keep the "business logic" separate from the message interface
    """

    def op_authenticate_user(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_authenticate_service_provider(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_add_service_provider(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_update_service_provider(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_revoke_service_provider(self, content, headers, msg):
        """ RU Service operation: .
        """


# Spawn of the process using the module name
factory = ProcessFactory(AuthorizationService)
