#!/usr/bin/env python

"""
@file ion/integration/ais/findDataResources/findDataResources.py
@author David Everett
@brief Worker class to find resources for a given user id, bounded by
spacial and temporal parameters.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import logging
from twisted.internet import defer

from decimal import Decimal

from ion.core.object import object_utils
from ion.core import ioninit

from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError
from ion.services.coi.resource_registry.association_client import AssociationClient

from ion.integration.ais.common.spatial_temporal_bounds import SpatialTemporalBounds
from ion.services.dm.inventory.association_service import AssociationServiceClient, AssociationServiceError
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, SUBJECT_PREDICATE_QUERY_TYPE, IDREF_TYPE
from ion.services.dm.distribution.events import DatasourceChangeEventSubscriber, \
                                                DatasetChangeEventSubscriber

from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, \
            DATASET_RESOURCE_TYPE_ID

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient                                                         

CONF = ioninit.config(__name__)

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
LCS_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=26, version=1)

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       FIND_DATA_RESOURCES_RSP_MSG_TYPE, \
                                                       FIND_DATA_RESOURCES_BY_OWNER_RSP_MSG_TYPE, \
                                                       FIND_DATA_SUBSCRIPTIONS_REQ_TYPE

#
# Dataset download URL initialization: can be specified in res/config/ionlocal.config 
#
DNLD_BASE_THREDDS_URL = CONF['DNLD_BASE_THREDDS_URL']
DNLD_DIR_PATH = CONF['DNLD_DIR_PATH']
DNLD_FILE_TYPE = CONF['DNLD_FILE_TYPE']

if DNLD_BASE_THREDDS_URL is None:
    DNLD_BASE_THREDDS_URL = 'http://thredds.oceanobservatories.org/thredds'
    log.error('DNLD_BASE_THREDDS_URL not set in ion.config or ionlocal.config!  Using %s' %(DNLD_BASE_THREDDS_URL))

if DNLD_DIR_PATH is None:
    DNLD_DIR_PATH = '/dodsC/ooiciData/'
    log.error('DNLD_DIR_PATH not set in ion.config or ionlocal.config!  Using %s' %(DNLD_DIR_PATH))
    
if DNLD_FILE_TYPE is None:
    DNLD_FILE_TYPE = '.ncml.html'
    log.error('DNLD_FILE_TYPE not set in ion.config or ionlocal.config!  Using %s' %(DNLD_FILE_TYPE))

class DatasetUpdateEventSubscriber(DatasetChangeEventSubscriber):
    def __init__(self, *args, **kwargs):
        self.ais = kwargs.get('process')
        self.metadataCache = self.ais.metadataCache
        DatasetChangeEventSubscriber.__init__(self, *args, **kwargs)

        self._hook_for_testing = defer.Deferred()


    @defer.inlineCallbacks
    def ondata(self, data):

        dSetResID = data['content'].additional_data.dataset_id
        log.debug("DatasetUpdateEventSubscriber received event for dsetID: %s" %(dSetResID))

        #
        # If the dataset is cached, delete it.  In any case, cache the dataset.
        #

        #
        # Delete the dataset
        #
        log.debug('DatasetUpdateEventSubscriber deleting %s' %(dSetResID))
        yield self.metadataCache.deleteDSetMetadata(dSetResID)

        #
        # Now  reload the dataset and datasource metadata
        #
        log.debug('DatasetUpdateEventSubscriber putting new metadata in cache')
        yield self.metadataCache.putDSetMetadata(dSetResID)

        log.debug("DatasetUpdateEventSubscriber event for dsetID: %s exit" %(dSetResID))

        if not self._hook_for_testing.called:
            self._hook_for_testing.callback(True)

class DatasourceUpdateEventSubscriber(DatasourceChangeEventSubscriber):
    def __init__(self, *args, **kwargs):
        self.ais = kwargs.get('process')
        self.metadataCache = self.ais.metadataCache
        DatasourceChangeEventSubscriber.__init__(self, *args, **kwargs)

        self._hook_for_testing = defer.Deferred()


    @defer.inlineCallbacks
    def ondata(self, data):

        dSourceResID = data['content'].additional_data.datasource_id
        log.debug(">>>----> DatasourceUpdateEventSubscriber received event for dsrcID: %s\n" %(dSourceResID))

        #
        # If the datasource is cached, delete it.  In any case, cache the
        # datasource.
        #

        log.debug('DatasourceUpdateEventSubscriber deleting %s' %(dSourceResID))
        yield self.metadataCache.deleteDSourceMetadata(dSourceResID)

        #
        # Now  reload the datasource metadata
        #
        log.debug('DatasourceUpdateEventSubscriber putting new metadata in cache')
        yield self.metadataCache.putDSourceMetadata(dSourceResID)

        #
        # Get the latest datasource metadata
        #

        dSource = yield self.ais.rc.get_instance(dSourceResID)
            
        #
        # Get the assoicated dataset(s) and refresh them (they might not be loaded
        # into cache because the datasource might have been inactive before this)
        #
        datasetList = yield self.metadataCache.getAssociatedDatasets(dSource)
        log.debug('getAssociatedDatasets returned %s' %(datasetList))
        for dSetResID in datasetList:
            # Should be only one in the list!!!
            dSet = yield self.metadataCache.getDSetMetadata(dSetResID)
            #
            # if the DSOURCE_ID is None, the datasource wasn't active or
            # didn't show up in the association service when the event for
            # the dataset was received, so refresh the dataset.
            #
            
            if dSet is None:
                log.info('DataSet Metadata not found %s; refreshing dataset' %(dSetResID))

                #
                # Now  load the dataset metadata
                #
                log.debug('DatasourceUpdateEventSubscriber loading dataset %s metadata into cache' %(dSetResID))
                yield self.metadataCache.putDSetMetadata(dSetResID)

            elif dSet.get('DSOURCE_ID') != dSourceResID:
                log.info('DataSet Metadata DSOURCE_ID was none or did not match for dSetID %s; refreshing dataset' %(dSetResID))
                #
                # Delete the dataset
                #
                log.debug('DatasourceUpdateEventSubscriber deleting dataset %s' %(dSetResID))
                yield self.metadataCache.deleteDSetMetadata(dSetResID)

                #
                # Now  reload the dataset and datasource metadata
                #
                log.debug('DatasourceUpdateEventSubscriber loading dataset %s metadata into cache' %(dSetResID))
                yield self.metadataCache.putDSetMetadata(dSetResID)

        if not self._hook_for_testing.called:
            self._hook_for_testing.callback(True)

        log.debug("<----<<<  DatasourceUpdateEventSubscriber event for dsourceID: %s exit" %(dSourceResID))

    
class FindDataResources(object):

    #
    # Flags to pass to __getDataResources()
    #
    ALL = 0
    BY_USER = 1
    
    #
    # Resource states to pass as parameters to self.__getDataResources()
    #
    PRIVATE = 0
    PUBLIC = 1
    
    def __init__(self, ais):
        log.info('FindDataResources.__init__()')
        self.ais = ais
        self.rc = ResourceClient(proc=ais)
        self.mc = ais.mc
        self.asc = AssociationServiceClient(proc=ais)
        self.ac = AssociationClient(proc=ais)
        self.nac = NotificationAlertServiceClient(proc=ais)

        self.__subscriptionList = None
        self.metadataCache = ais.getMetadataCache()

    @defer.inlineCallbacks
    def findDataResources(self, msg):
        """
        Worker class method called by app_integration_service to implement
        findDataResources.  Finds all dataset resources that are public,
        regardless of who owns them, as well as all dataset resources that
        are private and are owned by the calling user, and returns a list
        of their IDs along with a load of metadata.
        """

        log.debug('findDataResources Worker Class Method')

        # check that the GPB is correct type & has a payload
        result = yield self._CheckRequest(msg)
        if result != None:
            result.error_str = "AIS.findDataResources: " + result.error_str
            defer.returnValue(result)
            
        if msg.message_parameters_reference.IsFieldSet('user_ooi_id'):
            userID = msg.message_parameters_reference.user_ooi_id
        else:
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS findDataResources error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "AIS.findDataResources: Required field [user_ooi_id] not found in message"
            defer.returnValue(Response)
        
        self.downloadURL       = 'Uninitialized'
        yield self.__loadSubscriptionList(userID)
        
        #
        # Create the response message to which we will attach the list of
        # resource IDs
        #
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_RSP_MSG_TYPE)

        #
        # Iterate through the cache getting the datasets
        # TODO: This next few sets of code build up a list of metadata -
        # not just datasetIDs, but the whole metadata.  This is not a huge
        # deal, but if it stays this way, it might be a good idea to take
        # advantage of that fact by not getting the metadata again in
        # the private __getDataResources() method.  Or, just store the IDs
        # of the datasets here instead of the metadata.
        #
        dSetList = self.metadataCache.getDatasets()
        log.debug('findDataResources: cache contains %d datasets' %len(dSetList))
        
        #
        # Iterate through this list getting those owned by the userID
        #
        ownedByList = []
        for ds in dSetList:
            if ds['OwnerID'] == userID:
                ownedByList.append(ds)
                
        log.debug('findDataResources: ownedByList has %d datasets' %len(ownedByList))

        """
        Comment out in favor of selecting on datasets that are not already in the public list
        #
        # Now trim the list to only those that are private
        #
        ownedByAndPrivateList = []
        for ds in ownedByList:
            dSourceID = ds['DSourceID']
            dSource = yield self.metadataCache.getDSourceMetadata(dSourceID)
            if dSource is None:
                log.error('No corresponding datasource for datasetID: %s' %(ds['ResourceIdentity']))
            else:
                #
                # If the visibility is false, this is private, so add it to the list
                #
                if dSource['visibility'] == False:
                    log.debug('findDataResources: adding ds %s to ownedByAndPrivateList' %(ds['ResourceIdentity']))
                    ownedByAndPrivateList.append(ds)
            
        log.debug('findDataResources: ownedByAndPrivateList has %d datasets' %len(ownedByAndPrivateList))
        """

        #
        # We now have the list of owned by and private: now get the public datasets 
        #
        publicList = []
        for ds in dSetList:
            dSourceID = ds['DSourceID']
            dSource = yield self.metadataCache.getDSourceMetadata(dSourceID)
            if dSource is None:
                log.error('FindDataResources: No corresponding datasource for datasetID: %s' %(ds['ResourceIdentity']))
            else:
                #
                # If the visibility is true, this is public, so add it to the list
                #
                if dSource['visibility'] == True:
                    log.debug('findDataResources: adding ds %s to publicList' %(ds['ResourceIdentity']))
                    publicList.append(ds)
            
        log.debug('findDataResources: publicList has %d datasets' %len(publicList))

        #
        # Now add the two lists together
        #

        #finalList = ownedByAndPrivateList + publicList

        finalList = publicList
        for item in ownedByList:
            if item not in finalList:
                finalList.append(item)

        log.debug('findDataResources: finalList has %d datasets' %len(finalList))
        
        response = yield self.__getDataResources(msg, finalList, rspMsg, typeFlag = self.ALL)

        defer.returnValue(response)


    @defer.inlineCallbacks
    def findDataResourcesByUser(self, msg):
        """
        Worker class method called by app_integration_service to implement
        findDataResourcesByUser.  Finds all dataset resources regardless of state
        and returns their IDs along with a load of metadata.
        """

        log.debug('findDataResourcesByUser Worker Class Method')

        # check that the GPB is correct type & has a payload
        result = yield self._CheckRequest(msg)
        if result != None:
            result.error_str = "AIS.findDataResourcesByUser: " + result.error_str
            defer.returnValue(result)
            
        if msg.message_parameters_reference.IsFieldSet('user_ooi_id'):
            userID = msg.message_parameters_reference.user_ooi_id
        else:
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS findDataResourcesByUser error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "AIS.findDataResourcesByUser: Required field [user_ooi_id] not found in message"
            defer.returnValue(Response)

        self.downloadURL       = 'Uninitialized'
        
        #
        # Create the response message to which we will attach the list of
        # resource IDs
        #
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_BY_OWNER_RSP_MSG_TYPE)

        #
        # Iterate through the cache getting the datasets
        # TODO: This next few sets of code build up a list of metadata -
        # not just datasetIDs, but the whole metadata.  This is not a huge
        # deal, but if it stays this way, it might be a good idea to take
        # advantage of that fact by not getting the metadata again in
        # the private __getDataResources() method.  Or, just store the IDs
        # of the datasets here instead of the metadata.
        #
        dSetList = self.metadataCache.getDatasets()
        log.debug('findDataResourcesByUser: cache contains %d datasets' %len(dSetList))
        
        #
        # iterate through this list getting those owned by the userID
        #
        ownedByList = []
        for ds in dSetList:
            if ds['OwnerID'] == userID:
                ownedByList.append(ds)
                
        log.debug('findDataResourcesByUser: ownedByList has %d datasets' %len(ownedByList))

        response = yield self.__getDataResources(msg, ownedByList, rspMsg, typeFlag = self.BY_USER)
        
        defer.returnValue(response)


    @defer.inlineCallbacks
    def __getDataResources(self, msg, dSetList, rspMsg, typeFlag = ALL):
        """
        Given the list of datasetIDs, determine in the data represented by
        the dataset is within the given spatial and temporal bounds, and
        if so, add it to the response GPB.
        """

        log.debug('__getDataResources entry')        
        #
        # Instantiate a bounds object, and load it up with the given bounds
        # info
        #
        bounds = SpatialTemporalBounds()
        bounds.loadBounds(msg.message_parameters_reference)
        #userID = msg.message_parameters_reference.user_ooi_id       
        #
        # Now iterate through the list if dataset resource IDs and for each ID:
        #   - get the dataset instance
        #   - get the associated datasource instance
        #   - check that spatial and temporal criteria are met:
        #   - if not:
        #     - continue
        #   - if so:
        #     - add the metadata to the response GPB
        #        
        i = 0
        j = 0
        while i < len(dSetList):
            dSetResID = dSetList[i]['ResourceIdentity']
            log.debug('Working on dataset: ' + dSetResID)

            dSetMetadata = yield self.metadataCache.getDSetMetadata(dSetResID)
            if dSetMetadata is None:
                log.info('metadata not found for datasetID: ' + dSetResID)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                      MessageName='AIS findDataResources error response')
                Response.error_num = Response.ResponseCodes.NOT_FOUND
                Response.error_str = "AIS.findDataResources: Metadata not found."
                defer.returnValue(Response)
                    
            #
            # If the dataset's data is within the given criteria, include it
            # in the list
            #
            if bounds.isInBounds(dSetMetadata):
                if log.getEffectiveLevel() <= logging.DEBUG:
                    if 'title' in dSetMetadata.keys():
                        log.debug('dataset %s in bounds' % (dSetMetadata['title']))

                dSourceResID = dSetMetadata['DSourceID']
                if dSourceResID is None:
                    #
                    # There is no associated ID for this dataset; this is a strange
                    # error and it means that there was no datasource returned by
                    # the association service.  Really shouldn't happen, but it
                    # does sometimes.
                    #
                    log.error('dataset %s has no associated dSourceResID.' %(dSetResID))
                    i = i + 1
                    continue

                dSource = yield self.metadataCache.getDSourceMetadata(dSourceResID)
                if dSource is None:
                    #
                    # The datasource is not cached; this could be because it was deleted
                    # or because the datasource hasn't been added yet.  In any case,
                    # do not include the corresponding dataset in the list; continue
                    # the loop now (after incrementing index)
                    #
                    log.info('metadata not found for datasourceID: ' + dSourceResID)
                    i = i + 1
                    continue
                    
                #
                # Added this for Tim and Tom; not sure we need it yet...
                #
                ownerID = 'Is this used?'

                if typeFlag is self.ALL:
                    rspMsg.message_parameters_reference[0].dataResourceSummary.add()
                    #
                    # Set the notificationSet flag; this is not efficient at all
                    #
                    rspMsg.message_parameters_reference[0].dataResourceSummary[j].notificationSet = self.__isNotificationSet(dSetResID)
                    rspMsg.message_parameters_reference[0].dataResourceSummary[j].date_registered = dSource['registration_datetime_millis']
                        
                    self.__loadRspPayload(rspMsg.message_parameters_reference[0].dataResourceSummary[j].datasetMetadata, dSetMetadata, ownerID, dSetResID)
                    
                else:
                    rspMsg.message_parameters_reference[0].datasetByOwnerMetadata.add()
                    self.__loadRspByOwnerPayload(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata[j], dSetMetadata, ownerID, dSource)

                dSet = yield self.metadataCache.getDSet(dSetResID)
                self.__printRootAttributes(dSet)
                self.__printRootVariables(dSet)
                #self.__printSourceMetadata(dSource)
                #self.__printDownloadURL()
    
                j = j + 1
            else:
                if log.getEffectiveLevel() <= logging.DEBUG:
                    if 'title' in dSetMetadata.keys():
                        log.debug('dataset %s is OUT OF bounds' % (dSetMetadata['title']))
            
            i = i + 1

        defer.returnValue(rspMsg)        
        log.debug('__getDataResources exit')


    def __printDownloadURL(self):
        log.debug('Download URL: ' + self.downloadURL)


    def __printRootAttributes(self, ds):
        log.debug('Root Attributes:')
        for atrib in ds.root_group.attributes:
            log.debug('Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue())))


    def __printRootVariables(self, ds):
        log.debug('Root Variables:')
        for var in ds.root_group.variables:
            log.debug('Root Variable: %s' % str(var.name))
            for atrib in var.attributes:
                log.debug("Attribute: %s = %s" % (str(atrib.name), str(atrib.GetValue())))
            numDims = len(var.shape)                
            print "Variable %s has %d dimensions:" %(var.name, numDims)
            if numDims > 0:                
                for dim in var.shape:
                    log.debug("dim.name = %s, dim.source_name = %s, dim.length = %d" % (dim.name, dim.source_name, dim.length))


    def __printSourceMetadata(self, dSource):
        #log.debug('source_type: ' + str(dSource.source_type))
        log.debug('source_type: ' + str(dSource['source_type']))
        #for property in dSource.property:
        for property in dSource['property']:
            log.debug('Property: ' + property)
        #for sid in dSource.station_id:
        for sid in dSource['station_id']:
            log.debug('Station ID: ' + sid)
        #log.debug('request_type: ' + str(dSource.request_type))
        #log.debug('base_url: ' + dSource.base_url)
        #log.debug('max_ingest_millis: ' + str(dSource.max_ingest_millis))
        log.debug('request_type: ' + str(dSource['request_type']))
        log.debug('base_url: ' + dSource['base_url'])
        log.debug('max_ingest_millis: ' + str(dSource['max_ingest_millis']))


    def __loadRspPayload(self, rootAttributes, dSetMetadata, userID, dSetResID):
        rootAttributes.user_ooi_id = userID
        rootAttributes.data_resource_id = dSetResID
        rootAttributes.download_url = self.__createDownloadURL(dSetResID)
        for attrib in dSetMetadata:
            if attrib == 'dset':
                log.debug('Root Attribute is dset (dataset object); not printing')
            else:                
                log.debug('Root Attribute: %s = %s'  % (attrib, dSetMetadata[attrib]))
            if  attrib == 'title':
                rootAttributes.title = dSetMetadata[attrib]
            elif attrib == 'institution':                
                rootAttributes.institution = dSetMetadata[attrib]
            elif attrib == 'source':                
                rootAttributes.source = dSetMetadata[attrib]
            elif attrib == 'references':                
                rootAttributes.references = dSetMetadata[attrib]
            elif attrib == 'ion_time_coverage_start':                
                rootAttributes.ion_time_coverage_start = dSetMetadata[attrib]
            elif attrib == 'ion_time_coverage_end':                
                rootAttributes.ion_time_coverage_end = dSetMetadata[attrib]
            elif attrib == 'summary':                
                rootAttributes.summary = dSetMetadata[attrib]
            elif attrib == 'comment':                
                rootAttributes.comment = dSetMetadata[attrib]
            elif attrib == 'ion_geospatial_lat_min':                
                rootAttributes.ion_geospatial_lat_min = float(dSetMetadata[attrib])
            elif attrib == 'ion_geospatial_lat_max':                
                rootAttributes.ion_geospatial_lat_max = float(dSetMetadata[attrib])
            elif attrib == 'ion_geospatial_lon_min':                
                rootAttributes.ion_geospatial_lon_min = float(dSetMetadata[attrib])
            elif attrib == 'ion_geospatial_lon_max':                
                rootAttributes.ion_geospatial_lon_max = float(dSetMetadata[attrib])
            elif attrib == 'ion_geospatial_vertical_min':                
                rootAttributes.ion_geospatial_vertical_min = float(dSetMetadata[attrib])
            elif attrib == 'ion_geospatial_vertical_max':                
                rootAttributes.ion_geospatial_vertical_max = float(dSetMetadata[attrib])
            elif attrib == 'ion_geospatial_vertical_positive':                
                rootAttributes.ion_geospatial_vertical_positive = dSetMetadata[attrib]
            elif attrib == 'visualization_url':
                rootAttributes.visualization_url = dSetMetadata[attrib]


    def __createDownloadURL(self, dSetResID):
        #
        #  opendap URL for accessing the data.
        # The URL will be composed a couple parts:  <base_url_to_thredds> +
        # <directory_path> + <resourceid>.ncml
        #
        # http://localhost:8081/thredds/dodsC/scanData/<resID>.ncml
        #
        self.downloadURL =  DNLD_BASE_THREDDS_URL + \
                            DNLD_DIR_PATH + \
                            dSetResID + \
                            DNLD_FILE_TYPE

        log.debug('download URL for datasetID %s: %s' %(dSetResID, self.downloadURL))
        
        return self.downloadURL

    def __loadRspByOwnerPayload(self, rspPayload, dSetMetadata, userID, dSource):
        rspPayload.data_resource_id = dSetMetadata['ResourceIdentity']
        rspPayload.title = dSetMetadata['title']
        rspPayload.date_registered = dSource['registration_datetime_millis']
        rspPayload.ion_title = dSource['ion_title']
        #
        # Set the activate state based on the resource lcs
        #
        if dSource['visibility'] == True:
            rspPayload.activation_state = 'Public'
        else:
            rspPayload.activation_state = 'Private'
            
        rspPayload.update_interval_seconds = dSource['update_interval_seconds']

    @defer.inlineCallbacks
    def __loadSubscriptionList(self, userID):        
        """
        Get the list of subscriptions for the given user and save it in the
        private global __subscriptionList variable.
        """
        
        log.debug('__loadSubscriptionList()')
        reqMsg = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_SUBSCRIPTIONS_REQ_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id  = userID

        reply = yield self.nac.getSubscriptionList(reqMsg)
        self.__subscriptionList = reply.message_parameters_reference[0].subscriptionListResults


    def __isNotificationSet(self, dSetID):
        """
        Iterate through the user's list of subscriptions and test for the given
        dataset ID; it it's there, a subscription exists for the dataset.  The
        user's list of subscriptions in loaded before this is called.
        """
        
        log.debug('__isNotificationSet()')
        
        for subscription in self.__subscriptionList:
            if dSetID == subscription.subscriptionInfo.data_src_id:
                return True
            
        return False
        
 
    @defer.inlineCallbacks
    def _CheckRequest(self, request):
       # Check for correct request protocol buffer type
       if request.MessageType != AIS_REQUEST_MSG_TYPE:
          # build AIS error response
          Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
          Response.error_num = Response.ResponseCodes.BAD_REQUEST
          Response.error_str = 'Bad message type receieved, ignoring'
          defer.returnValue(Response)
 
       # Check payload in message
       if not request.IsFieldSet('message_parameters_reference'):
          # build AIS error response
          Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
          Response.error_num = Response.ResponseCodes.BAD_REQUEST
          Response.error_str = "Required field [message_parameters_reference] not found in message"
          defer.returnValue(Response)
   
       defer.returnValue(None)
       

