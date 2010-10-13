#!/usr/bin/env python

"""
@file ion/services/dm/transformation/transformation_service.py
@author Michael Meisinger
@brief service for transforming information
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

class TransformationService(ServiceProcess):
    """Transformation service interface
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='transformation_service', version='0.1.0', dependencies=[])

    def op_transform(self, content, headers, msg):
        """Service operation: TBD
        """

# Spawn of the process using the module name
factory = ProcessFactory(TransformationService)

class TransformationClient(ServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'transformation_service'
        ServiceClient.__init__(self, proc, **kwargs)

    def transform(self, object):
        '''
        @brief transform an object to a different type
        @param what?
        '''
