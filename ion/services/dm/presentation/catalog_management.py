#!/usr/bin/env python

"""
@file ion/services/dm/presentation/catalog_management.py
@author
@brief A catalog is a new data set that aggregates and presents datasets in a specific way.
       The catalog definition services enables to define catalogs, which are created as datasets then can be accessed through the inventory.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect


from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class CatalogManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='catalog_mgmt',
                                             version='0.1.0',
                                             dependencies=[])


    def __init__(self, *args, **kwargs):

        ServiceProcess.__init__(self, *args, **kwargs)

        log.debug('CatalogManagementService.__init__()')


    @defer.inlineCallbacks
    def op_define_catalog(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = self.define_catalog(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_define_view(self, request, headers, msg):
        response = self.define_view(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_get_catalog_definition(self, request, headers, msg):
        response = self.get_catalog_definition(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)



    def define_catalog(self, catalogDefinition='default'):

        # Validate the input filter and augment context as required

        # TODO: define the paramters that comprise a catalog

        # Create catalog from input, set initial state, register

        # Create associations

        # Return a resource ref

        return

    def define_view(self, datasetReference='ref', viewDefinition='default'):

        # Validate the resource metadata  and augment context as required

        #  Create view resource and set initial state

        # Create associations

        # Return a resource ref

        return

    def get_catalog_definition(self, catalogRef='default'):

        # Validate request

        # Retrieve  current catalog settings

        # Return catalog metadata

        return



class CatalogManagementServiceClient(ServiceClient):

    """
    This is a service client for CatalogManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "catalog_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_catalog(self, catalogDefinition='default'):
        (content, headers, msg) = yield self.rpc_send('define_catalog', {'catalogDefinition':catalogDefinition})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def define_view(self, datasetReference='ref', viewDefinition='default'):
        (content, headers, msg) = yield self.rpc_send('define_view', {'datasetReference':datasetReference, 'viewDefinition':viewDefinition})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_catalog_definition(self, catalogRef='default'):
        (content, headers, msg) = yield self.rpc_send('get_catalog_definition', {'catalogRef':catalogRef})
        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(CatalogManagementService)
  
  