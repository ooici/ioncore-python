
#!/usr/bin/env python

"""
@file ion/services/sa/data_product_management.py
@author
@brief Services related to the activation and registry of data products
"""

from ion.core import ioninit
CONF = ioninit.config(__name__)

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect
import ion.util.procutils as pu

from ion.core.messaging import message_client
from ion.core.exception import ReceivedError, ApplicationError
from ion.core.data.store import Query

from ion.core.data import cassandra_bootstrap
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.dm.inventory.association_service import AssociationServiceClient

from ion.services.sa.data_acquisition_management.data_acquisition_management import DataAcquisitionManagementServiceClient

class DataProductManagementService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='data_product_mgmt',
                                             version='0.1.0',
                                             dependencies=[])
    
    DATA_PRODUCT_OOI_ID = 'data_product_ooi_id'
    DATA_PRODUCER_OOI_ID = 'data_producer_ooi_id'
    DATA_STREAM_OOI_ID = 'data_stream_ooi_id'
    TITLE = 'title'
    SUMMARY = 'summary'
    KEYWORDS = 'keywords'
    DATA_PRODUCER = 'data_producer'
    DATA_PRODUCER_NAME = 'data_producer_name'

    def __init__(self, *args, **kwargs):

        log.debug('DataProductManagementService.__init__()')
        ServiceProcess.__init__(self, *args, **kwargs)
        index_store_class_name = self.spawn_args.get('index_store_class', CONF.getValue('index_store_class', default='ion.core.data.store.IndexStore'))
        self.index_store_class = pu.get_class(index_store_class_name)
        self.damc = DataAcquisitionManagementServiceClient(proc = self)
        

    def slc_init(self):

        log.debug('DataProductManagementService.slc_init()')
        #initialize index store for data product information
        DATA_PRODUCT_INDEXED_COLUMNS = [self.DATA_PRODUCT_OOI_ID,
                                        self.DATA_PRODUCER_OOI_ID,
                                        self.DATA_STREAM_OOI_ID,
                                        self.TITLE,
                                        self.SUMMARY,
                                        self.KEYWORDS]
        
        if issubclass(self.index_store_class , cassandra_bootstrap.CassandraIndexedStoreBootstrap):
            log.info("CassandraStore not yet supported")
        else: 
            log.info("Instantiating Memory Store")
            self.index_store = self.index_store_class(self, indices=DATA_PRODUCT_INDEXED_COLUMNS )
 

    @defer.inlineCallbacks
    def op_define_data_product(self, request, headers, msg):

        assert(isinstance(request, dict))
        response = yield self.define_data_product(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_find_data_products(self, request, headers, msg):
        response = yield self.find_data_products(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_get_data_product_detail(self, request, headers, msg):
        response = yield self.get_data_product_detail(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_set_data_product_detail(self, request, headers, msg):
        response = yield self.set_data_product_detail(**request)  # Unpack dict to kwargs
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def define_data_product(self, ParameterDictionary):

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
        
        log.info("DataProductManagementService.define_data_product ParameterDictionary: %s ", str(ParameterDictionary))

        DataProductId = pu.create_guid()

        if self.DATA_PRODUCER in ParameterDictionary:
            result = yield self.damc.define_data_producer(producer=ParameterDictionary[self.DATA_PRODUCER])
            log.info("DataProductManagementService.define_data_product result: %s ", str(result))
            DataProducerId = result[self.DATA_PRODUCER_OOI_ID]
            DataStreamId = result[self.DATA_STREAM_OOI_ID]
        else:
            DataProducerId = ''
            DataStreamId = ''            
            
        if self.TITLE in ParameterDictionary:
            title = ParameterDictionary[self.TITLE]
        else:
            title = ''
        if self.SUMMARY in ParameterDictionary:
            summary = ParameterDictionary[self.SUMMARY]
        else:
            summary = ''
        if self.KEYWORDS in ParameterDictionary:
            keywords = ParameterDictionary[self.KEYWORDS]
        else:
            keywords = ''
            
        attributes = {self.DATA_PRODUCT_OOI_ID : DataProductId,
                      self.DATA_PRODUCER_OOI_ID : DataProducerId,
                      self.DATA_STREAM_OOI_ID : DataStreamId,
                      self.TITLE : title,
                      self.SUMMARY : summary,
                      self.KEYWORDS : keywords}
        log.info("DataProductManagementService.define_data_product attributes: %s ", str(attributes))
        yield self.index_store.put(DataProductId, DataProductId, attributes)
        defer.returnValue({'Response':'OK', self.DATA_PRODUCT_OOI_ID:DataProductId})


    @defer.inlineCallbacks
    def find_data_products(self, filter={}):

        # Validate the input filter and augment context as required

        # Define set of resource attributes to filter on, change parameter from "filter" to include attributes and filter values.
        #     potentially: title, keywords, date_created, creator_name, project, geospatial coords, time range

        # Call DM DiscoveryService to query the catalog for matches

        # Organize and return the list of matches with summary metadata (title, summary, keywords)
        log.info("DataProductManagementService.find_data_products filter: %s ", str(filter))
        #find the item in the store
        query = Query()
        for item in filter:
            log.debug("item = " + str(item) + "   value = " + str(filter[item]))
            query.add_predicate_eq(item, filter[item])
        rows = yield self.index_store.query(query)
        log.info("DataProductManagementService.find_data_products  query returned %s " % (rows))
        DataProducts = []
        for key, row in rows.iteritems () :
            DataProduct = []
            DataProduct.append(rows[key][self.TITLE])
            DataProduct.append(rows[key][self.DATA_PRODUCT_OOI_ID])
            DataProduct.append(rows[key][self.DATA_PRODUCER_OOI_ID])
            DataProduct.append(rows[key][self.DATA_STREAM_OOI_ID])
            DataProduct.append(rows[key][self.SUMMARY])
            DataProducts.append(DataProduct)
        defer.returnValue({'Response':'OK', 'products':DataProducts})


    @defer.inlineCallbacks
    def get_data_product_detail(self, data_product_ooi_id='default'):

        # Retrieve all metadata for a specific data product
        # Return data product resource

        query = Query()
        query.add_predicate_eq(self.DATA_PRODUCT_OOI_ID, data_product_ooi_id)
        rows = yield self.index_store.query(query)
        log.debug("DataProductManagementService.get_data_product_detail rows: %s ", str(rows))
        if data_product_ooi_id in rows:
            rows[data_product_ooi_id].pop('value', None)   # get rid of some stupid key that the index store adds
            defer.returnValue({'Response':'OK', 'product':rows[data_product_ooi_id]})
        else:
            defer.returnValue({'Response':'FAILURE'})


    @defer.inlineCallbacks
    def set_data_product_detail(self, ParameterDictionary):

        # Update metadata for a specific data product
        # Return updated data product resource

        log.info("DataProductManagementService.set_data_product_detail ParameterDictionary: %s ", str(ParameterDictionary))
        if not self.DATA_PRODUCT_OOI_ID in ParameterDictionary:
            defer.returnValue({'Response':'FAILURE'})
        else:
            productId = ParameterDictionary[self.DATA_PRODUCT_OOI_ID]
        query = Query()
        query.add_predicate_eq(self.DATA_PRODUCT_OOI_ID, productId)
        rows = yield self.index_store.query(query)
        log.debug("DataProductManagementService.set_data_product_detail rows: %s ", str(rows))
        if not productId in rows:
            defer.returnValue({'Response':'FAILURE'})
        product = rows[productId]
        product.pop('value', None)    # get rid of some stupid key that the index store adds
        if product[self.DATA_PRODUCER_OOI_ID] == '':
            if self.DATA_PRODUCER in ParameterDictionary:
                result = yield self.damc.define_data_producer(producer=ParameterDictionary[self.DATA_PRODUCER])
                log.info("DataProductManagementService.set_data_product_detail result: %s ", str(result))
                product[self.DATA_PRODUCER_OOI_ID] = result[self.DATA_PRODUCER_OOI_ID]
                product[self.DATA_STREAM_OOI_ID] = result[self.DATA_STREAM_OOI_ID]
        if self.TITLE in ParameterDictionary:
            product[self.TITLE] = ParameterDictionary[self.TITLE]
        if self.SUMMARY in ParameterDictionary:
            product[self.SUMMARY] = ParameterDictionary[self.SUMMARY]
        if self.KEYWORDS in ParameterDictionary:
            product[self.KEYWORDS] = ParameterDictionary[self.KEYWORDS]
        yield self.index_store.put(productId, productId, product)
        defer.returnValue({'Response':'OK', 'product':product})


class DataProductManagementServiceClient(ServiceClient):

    """
    This is a service client for DataProductManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_product_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_data_product(self, ParameterDictionary={}):
        # parameters: title, summary, keywords, data_producer):
        log.debug("DataProductManagementServiceClient:define_data_product")
        (content, headers, msg) = yield self.rpc_send('define_data_product', {'ParameterDictionary':ParameterDictionary})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def find_data_products(self, filter={}):
        (content, headers, msg) = yield self.rpc_send('find_data_products', {'filter':filter})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_data_product_detail(self, data_product_ooi_id='default'):
        (content, headers, msg) = yield self.rpc_send('get_data_product_detail', {'data_product_ooi_id':data_product_ooi_id})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_data_product_detail(self, ParameterDictionary={}):
        # parameters: title, summary, keywords, data_producer):
        (content, headers, msg) = yield self.rpc_send('set_data_product_detail', {'ParameterDictionary':ParameterDictionary})
        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(DataProductManagementService)
  