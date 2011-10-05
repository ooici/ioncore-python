#!/usr/bin/env python

"""
@file ion/services/sa/data_process_management/data_process_management.py
@author
@brief Data processing services enable the derivation of information from lower level information, on a continuous data streaming basis, for instance for the generation of derived data products.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect


from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class DataProcessManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='data_process_mgmt',
                                             version='0.1.0',
                                             dependencies=[])


    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('DataProcessManagementService.__init__()')


    @defer.inlineCallbacks
    def op_attach_process(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.attach_process(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_define_data_process(self, request, headers, msg):
        response = self.define_data_process(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_get_data_process(self, request, headers, msg):
        response = self.get_data_process(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    def attach_process(self, process='default'):

        # Connect the process to the input and output streams

        # Call DM:DataTransformMgmtSvc:DefineTransform to configure

        # Call DM:DataTransformMgmtSvc:BindTransform to connect transform and execute



        return

    def define_data_process(self, process='default'):

        # Register the data process; create and store the resource and associations

        # Coordinate orchestration with CEI:ProcessMgmtSvc to define a process

        #



        return


    def get_data_process(self, process='default'):

        # Locate a process based on metadata filters



        return



class DataProcessManagementServiceClient(ServiceClient):

    """
    This is a service client for DataProcessManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_process_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def attach_process(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('attach_process', {'process':process})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def define_data_process(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('define_data_process', {'process':process})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_data_process(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('get_data_process', {'process':process})
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(DataProcessManagementService)

  
  