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

    @defer.inlineCallbacks
    def op_get_data_product_detail(self, request, headers, msg):
        response = self.get_data_product_detail(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_set_data_product_detail(self, request, headers, msg):
        response = self.set_data_product_detail(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    def define_data_product(self, title='title', summary='summary', keywords='keywords'):

        # DefineDataProduct will validate and register a new data product within the system

        # Extend parameter list to include full resource definition which may include:
        # Metadata (resource definition in YAML) may include (see OOI Metadata Planning, Approach and Progress document from KStocks)
        # title
        # summary : A paragraph describing the dataset.
        # keywords : A comma separated list of key words and phrases.
        # naming_authority : The combination of the "naming authority" and the "id" should be a globally unique identifier for the dataset.
        # keywords_vocabulary : If you are following a guideline for the words/phrases in your "keywords" attribute, put the name of that guideline here.
        # cdm_data_type - The THREDDS data type appropriate for this dataset.
        # history: Provides an audit trail for modifications to the original data.
        # comment: Miscellaneous information about the data.
        # date_created: The date on which the data was created.
        # creator_name, creator_url, creator_email, institution: The data creator's name, URL, and email. The "institution" attribute will be used if the "creator_name" attribute does not exist.
        # project: The scientific project that produced the data.
        # processing_level: A textual description of the processing (or quality control) level of the data.
        # acknowledgement: A place to acknowledge various type of support for the project that produced this data.
        # geospatial_lat_min, lat_max, lon_min, lon_max: Describes a simple latitude, longitude, and vertical bounding box. For a more detailed geospatial coverage, see the suggested geospatial attributes.
        # time_coverage_start, _end, _duration, _resolution: Describes the temporal coverage of the data as a time range.
        # standard_name_vocabulary: The name of the controlled vocabulary from which variable standard names are taken.
        # license: Describe the restrictions to data access and distribution.

        # Validate - TBD by the work that Karen Stocks is driving with John Graybeal

        # Register - create and store a new DataProduct resource using provided metadata

        # Create necessary associations to owner, instrument, etc

        # Call Data Aquisition Mgmt Svc:define_data_producer to coordinate creation of topic and connection to source

        # Return a resource ref

        return

    def find_data_product(self, filter='default'):

        # Validate the input filter and augment context as required

        # Define set of resource attributes to filter on, change parameter from "filter" to include attributes and filter values.
        #     potentially: title, keywords, date_created, creator_name, project, geospatial coords, time range

        # Call DM DiscoveryService to query the catalog for matches

        # Organize and return the list of matches with summary metadata (title, summary, keywords)

        return

    def get_data_product_detail(self, productId='default'):

        # Retrieve all metadata for a specific data product

        # Return data product resource

        return

    def set_data_product_detail(self, productId='default', title='title', summary='summary', keywords='keywords'):

        # Update  metadata for a specific data product

        # Return updated data product resource

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
    def define_data_product(self, title='title', summary='summary', keywords='keywords'):
        (content, headers, msg) = yield self.rpc_send('define_data_product', {'title':title, 'summary':summary, 'keywords':keywords})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def find_data_product(self, filter='default'):
        (content, headers, msg) = yield self.rpc_send('find_data_product', {'filter':filter})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_data_product_detail(self, productId='default'):
        (content, headers, msg) = yield self.rpc_send('get_data_product_detail', {'productId':productId})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_data_product_detail(self, productId='default', title='title', summary='summary', keywords='keywords'):
        (content, headers, msg) = yield self.rpc_send('set_data_product_detail', {'title':title, 'summary':summary, 'keywords':keywords})
        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(DataProductManagementService)
  