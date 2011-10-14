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



    @defer.inlineCallbacks
    def op_bind_transform(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.bind_transform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_define_transform(self, request, headers, msg):
        response = self.define_transform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_schedule_transform(self, request, headers, msg):
        response = self.schedule_transform(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    def bind_transform(self, process='default'):

        # Connect the process to the input and output streams


        return

    def define_transform(self, resourceRef='processResource'):

        # Create and register the transform process, create  associations

        # Coordinate orchestration with CEI:ProcessMgmtSvc to configure the supporting process

        # Return resource reference

        return


    def schedule_transform(self, process='default'):

        # Initiate process execution

        return






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

    @defer.inlineCallbacks
    def bind_transform(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('bind_transform', {'process':process})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def define_transform(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('define_transform', {'process':process})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def schedule_transform(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('schedule_transform', {'process':process})
        defer.returnValue(content)
