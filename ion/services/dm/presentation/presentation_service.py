#!/usr/bin/env python

"""
@file ion/services/dm/presentation/presentation_service.py
@author David Stuebe
@brief service for presentation of DM resources and services.
"""



import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from ion.core.cc.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

class PresentationService(BaseService):
    """Presentation service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='presentation_service', version='0.1.0', dependencies=[])
 
    def op_present_catalog(self, content, headers, msg):
        """Service operation: TBD
        """
        
# Spawn of the process using the module name
factory = ProtocolFactory(PresentationService)


class PresentationClient(BaseServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'presentation_service'
        BaseServiceClient.__init__(self, proc, **kwargs)

    def present_catalog(self, resources):
        '''
        @brief present a catalog of resources
        @param what?
        '''