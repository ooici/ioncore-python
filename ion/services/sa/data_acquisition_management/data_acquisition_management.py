#!/usr/bin/env python

"""
@file ion/services/sa/data_acquisition_management/data_acquisition_management.py
@author
@brief Services related to the transformation of data samples and engineering data into messages with metadata ready for publication as data product and ingestion into data archives.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect


from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class DataAcquisitionManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='data_acquisition_mgmt',
                                             version='0.1.0',
                                             dependencies=[])


    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('DataAcquisitionManagementService.__init__()')


    @defer.inlineCallbacks
    def op_define_data_agent(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.define_data_agent(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_define_data_producer(self, request, headers, msg):
        response = self.define_data_producer(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    def define_data_agent(self, agent='default'):

        # Register the data agent; create resource with metadata and associations



        return

    def define_data_producer(self, producer='default'):

        # Register the data producer; create and store the resource and associations (eg instrument or process that is producing)


        # Coordinate creation of the stream channel; call PubsubMgmtSvc with characterization of data stream to define the topic and the producer
        

        # Return the XP information to the data agent

        return




class DataAcquisitionManagementServiceClient(ServiceClient):

    """
    This is a service client for DataAcquisitionManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_acquisition_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_data_agent(self, agent='default'):
        (content, headers, msg) = yield self.rpc_send('define_data_agent', {'agent':agent})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def define_data_producer(self, producer='default'):
        (content, headers, msg) = yield self.rpc_send('define_data_producer', {'producer':producer})
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(DataAcquisitionManagementService)

  