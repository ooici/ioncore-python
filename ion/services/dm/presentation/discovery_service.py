#!/usr/bin/env python

"""
@file ion/services/dm/presentation/discovery_service.py
@author
@brief The Discovery service supports finding resources by metadata attributes, potentially applying semantic reasoning
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

class DiscoveryService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='discovery',
                                             version='0.1.0',
                                             dependencies=[])


    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('DiscoveryService.__init__()')


    @defer.inlineCallbacks
    def op_define_search(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.define_search(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_find_by_metadata(self, request, headers, msg):
        response = self.find_by_metadata(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_find_by_type(self, request, headers, msg):
        response = self.find_by_type(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)



    def define_search(self, query='query'):

        # Create query definition

        # Validate search type and filter variables
        
        # Construct  query across catalogs and associations

        # Organize and return the list of matches with summary metadata

        return

    def find_by_metadata(self, resourceTypes='resource',keyValueFilters='default:default'):

        # Query for resources and data, filtering on metadata

        # Validate the input filter and augment context as required

        # Construct query across resource registery and data sets for metadata matches

        # Organize and return the list of matches with summary metadata

        return

    def find_by_type(self, keyValueFilters='default:default'):

        # Locate resources or data based on type attributes

        # Validate the input filter and augment context as required

        # Construct query across resource registry and data sets for matches in types

        # Organize and return the list of matches with summary metadata

        return



class DiscoveryServiceClient(ServiceClient):

    """
    This is a service client for DiscoveryServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "discovery"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_search(self, query='query'):
        (content, headers, msg) = yield self.rpc_send('define_search', {'query':query})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def find_by_metadata(self, resourceTypes='resource',keyValueFilters='default:default'):
        (content, headers, msg) = yield self.rpc_send('find_by_metadata', {'resourceTypes':resourceTypes, 'keyValueFilters':keyValueFilters})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def find_by_type(self, keyValueFilters='default:default'):
        (content, headers, msg) = yield self.rpc_send('find_by_type', {'keyValueFilters':keyValueFilters})
        defer.returnValue(content)





# Spawn of the process using the module name
factory = ProcessFactory(DiscoveryService)
  