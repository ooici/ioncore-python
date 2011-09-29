#!/usr/bin/env python

"""
@file ion/services/sa/data_product_management.py
@author
@brief Services related to the activation and registry of data products
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect

from ion.core.messaging import message_client
from ion.core.exception import ReceivedError, ApplicationError

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.dm.inventory.association_service import AssociationServiceClient

class DataProductManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='data_product_mgmt',
                                             version='0.1.0',
                                             dependencies=[])


    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('DataProductManagementService.__init__()')


    @defer.inlineCallbacks
    def op_define_data_product(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.define_data_product(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_find_data_product(self, request, headers, msg):
        response = self.find_data_product(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    def define_data_product(self, owner='default', source='default', description='default desc'):

        # DefineDataProduct will validate and register a new data product within the system

        # Validate - TBD by the work that Karen Stocks is driving with John Graybeal

        # Register - create and store a new DataProduct resource using provided metadata

        # Create necessary associations to owner, instrument, etc

        # Call Data Aquisition Mgmt Svc:define_data_producer to coordinate creation of topic and connection to source

        # Return a resource ref

        return

    def find_data_product(self, filter='default'):

        # Validate the input filter and augment context as required

        # Call DM DiscoveryService to query the catalog for matches

        # Organize and return the list of matches


        return




class DataProductManagementServiceClient(ServiceClient):

    """
    This is a service client for DataProductManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_product_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_data_product(self, owner='owner', source='source', description='description'):
        (content, headers, msg) = yield self.rpc_send('define_data_product', {'owner':owner, 'source':source, 'description':description})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def find_data_product(self, filter='default'):
        (content, headers, msg) = yield self.rpc_send('find_data_product', {'filter':filter})
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(DataProductManagementService)
  