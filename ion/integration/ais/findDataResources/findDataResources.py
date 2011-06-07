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
from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError
from ion.services.coi.resource_registry.association_client import AssociationClient
from ion.services.coi.resource_registry.association_client import AssociationClientError

from ion.integration.ais.common.spatial_temporal_bounds import SpatialTemporalBounds
from ion.services.dm.inventory.association_service import AssociationServiceClient, AssociationServiceError
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, SUBJECT_PREDICATE_QUERY_TYPE, IDREF_TYPE
from ion.services.dm.distribution.events import DatasetSupplementAddedEventSubscriber

from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, TYPE_OF_ID, HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, \
            DATASET_RESOURCE_TYPE_ID

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient                                                         

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


DNLD_BASE_THREDDS_URL = 'http://thredds.oceanobservatories.org/thredds'
DNLD_DIR_PATH = '/dodsC/ooiciData/'
DNLD_FILE_TYPE = '.ncml.html'

class DataResourceUpdateEventSubscriber(DatasetSupplementAddedEventSubscriber):
    def __init__(self, ais, *args, **kwargs):
        self.msgs = []
        self.metadataCache = ais.getMetadataCache()
        DatasetSupplementAddedEventSubscriber.__init__(self, *args, **kwargs)

                
    @defer.inlineCallbacks
    def ondata(self, data):
        log.debug("DataResourceUpdateEventSubscriber received a message:\n")

        #
        # Don't have any way to get the datasource ID (from the trial test),
        # so for for now get the cached dataset metadata and get the source
        #
        dSetResID = data['content'].additional_data.dataset_id
        #dSourceResID = data['content'].additional_data.datasource_id
        dSetMetadata = yield self.metadataCache.getDSetMetadata(dSetResID)

        #
        # If dataset does not exist, this must be a new dataset; skip the
        # delete step.
        #
        if dSetMetadata is not None:
            dSourceResID = dSetMetadata['DSourceID']

            #
            # Delete the dataset and datasource metadata
            #
            log.debug('deleting %s, %s from metadataCache' %(dSetResID, dSourceResID))
            yield self.metadataCache.deleteDSetMetadata(dSetResID)
            yield self.metadataCache.deleteDSourceMetadata(dSourceResID)

        else:
            dSourceResID = data['content'].additional_data.datasource_id

        #
        # Now  reload the dataset and datasource metadata
        #
        log.debug('putting new metadata in cache')
        yield self.metadataCache.putDSetMetadata(dSetResID)
        yield self.metadataCache.putDSourceMetadata(dSourceResID)
    
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
        self.bUseMetadataCache = True

    @defer.inlineCallbacks
    def findDataResources(self, msg):
        """
        Worker class method called by app_integration_service to implement
        findDataResources.  Finds all dataset resources that are "published"
        and returns their IDs along with a load of metadata.
        """

        log.debug('findDataResources Worker Class Method')

        if msg.message_parameters_reference.IsFieldSet('user_ooi_id'):
            userID = msg.message_parameters_reference.user_ooi_id
        else:
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS findDataResources error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "Required field [user_ooi_id] not found in message"
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

        # Get the list of PRIVATE dataset resource IDs by owner
        dSetResults = yield self.__findPrivateDatasetResourcesByOwner(userID)
        if dSetResults == None:
            log.error('Error finding resources.')
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS findDataResources error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "No DatasetIDs were found."
            defer.returnValue(Response)
            
        dSetList = dSetResults.idrefs
        
        log.debug('Dataset list contains ' + str(len(dSetList)) + ' private datasets owned by ' + str(userID))

        # Get the list of PUBLIC dataset resource IDs
        dSetResults = yield self.__findResourcesOfType(DATASET_RESOURCE_TYPE_ID, self.PUBLIC)
        if dSetResults == None:
            log.error('Error finding resources.')
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS findDataResources error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "No DatasetIDs were found."
            defer.returnValue(Response)

        log.debug('Dataset list contains ' + str(len(dSetResults.idrefs)) + ' public datasets.')

        #
        # Add the PUBLIC datasets to the list of private datasets
        #
        i = len(dSetList)
        j = 0
        for dSetID in dSetResults.idrefs:
            dSetList.add()
            dSetList[i] = dSetResults.idrefs[j]
            i = i + 1
            j = j + 1

        log.debug('Dataset list contains ' + str(len(dSetList)) + ' total datasets.')

        yield self.__getDataResources(msg, dSetList, rspMsg, typeFlag = self.ALL)

        defer.returnValue(rspMsg)


    @defer.inlineCallbacks
    def findDataResourcesByUser(self, msg):
        """
        Worker class method called by app_integration_service to implement
        findDataResourcesByUser.  Finds all dataset resources regardless of state
        and returns their IDs along with a load of metadata.
        """

        log.debug('findDataResourcesByUser Worker Class Method')

        if msg.message_parameters_reference.IsFieldSet('user_ooi_id'):
            userID = msg.message_parameters_reference.user_ooi_id
        else:
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS findDataResourcesByUser error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "Required field [user_ooi_id] not found in message"
            defer.returnValue(Response)

        self.downloadURL       = 'Uninitialized'
        
        #
        # Create the response message to which we will attach the list of
        # resource IDs
        #
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_BY_OWNER_RSP_MSG_TYPE)

        # Get the list of dataset resource IDs
        dSetResults = yield self.__findResourcesOfTypeAndOwner(DATASET_RESOURCE_TYPE_ID, userID)
        if dSetResults == None:
            log.error('Error finding resources.')
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS findDataResources error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "No DatasetIDs were found."
            defer.returnValue(Response)
        
        log.debug('Found ' + str(len(dSetResults.idrefs)) + ' datasets.')

        yield self.__getDataResources(msg, dSetResults.idrefs, rspMsg, typeFlag = self.BY_USER)
        
        defer.returnValue(rspMsg)


    @defer.inlineCallbacks
    def getAssociatedSource(self, dSetResID):
        """
        Worker class method to get the data source that associated with a given
        data set.  This is a public method because it can be called from the
        findDataResourceDetail worker class.
        """
        log.debug('getAssociatedSource() entry')

        try: 
            ds = yield self.rc.get_instance(dSetResID)
            
        except ResourceClientError:    
            log.error('AssociationError')
            defer.returnValue(None)

        try:
            results = yield self.ac.find_associations(obj=ds, predicate_or_predicates=HAS_A_ID)

        except AssociationClientError:
            log.error('AssociationError')
            defer.returnValue(None)

        for association in results:
            log.debug('Associated Source for Dataset: ' + \
                      association.ObjectReference.key + \
                      ' is: ' + association.SubjectReference.key)

        log.debug('getAssociatedSource() exit')

        defer.returnValue(association.SubjectReference.key)

                      
    @defer.inlineCallbacks
    def getAssociatedOwner(self, dsID):
        """
        Worker class method to find the owner associated with a data set.
        This is a public method because it can be called from the
        findDataResourceDetail worker class.
        """
        log.debug('getAssociatedOwner() entry')

        request = yield self.mc.create_instance(SUBJECT_PREDICATE_QUERY_TYPE)

        #
        # Set up an owned_by_id search term using:
        # - OWNED_BY_ID as predicate
        # - LCS_REFERENCE_TYPE object set to ACTIVE as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # ..(subject)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = dsID
        
        pair.subject = type_ref

        log.info('Calling get_objects with dsID: ' + dsID)

        try:
            result = yield self.asc.get_objects(request)
        
        except AssociationServiceError:
            log.error('getAssociatedOwner: association error!')
            defer.returnValue(None)

        if len(result.idrefs) == 0:
            log.error('Owner not found!')
            defer.returnValue('OWNER NOT FOUND!')
        elif len(result.idrefs) == 1:
            log.debug('getAssociatedOwner() exit')
            defer.returnValue(result.idrefs[0].key)
        else:
            log.error('More than 1 owner found!')
            defer.returnValue('MULTIPLE OWNERS!')


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
            dSetResID = dSetList[i].key
            log.debug('Working on dataset: ' + dSetResID)

            if self.bUseMetadataCache:            
                dSetMetadata = yield self.metadataCache.getDSetMetadata(dSetResID)
                if dSetMetadata is None:
                    log.info('metadata not found for datasetID: ' + dSetResID)
                    Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                          MessageName='AIS findDataResources error response')
                    Response.error_num = Response.ResponseCodes.NOT_FOUND
                    Response.error_str = "Metadata not found."
                    defer.returnValue(Response)
                    
            else:                    
                dSet = yield self.rc.get_instance(dSetResID)
                if dSet is None:
                    log.error('dSet is None')
                    Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                          MessageName='AIS findDataResources error response')
                    Response.error_num = Response.ResponseCodes.NOT_FOUND
                    Response.error_str = "Dataset not found."
                    defer.returnValue(Response)        
    
                dSetMetadata = {}
                self.__loadMinMetaData(dSet, dSetMetadata)


            #
            # If the dataset's data is within the given criteria, include it
            # in the list
            #
            if bounds.isInBounds(dSetMetadata):
                if log.getEffectiveLevel() <= logging.DEBUG:
                    if 'title' in dSetMetadata.keys():
                        log.debug('dataset %s in bounds' % (dSetMetadata['title']))

                if self.bUseMetadataCache:            
                    dSourceResID = dSetMetadata['DSourceID']
                    if dSourceResID is None:
                        log.info('dSourceResID is None')
                        Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                              MessageName='AIS findDataResources error response')
                        Response.error_num = Response.ResponseCodes.NOT_FOUND
                        Response.error_str = "Datasource not found."
                        defer.returnValue(Response)

                    dSource = yield self.metadataCache.getDSetMetadata(dSourceResID)
                    if dSource is None:
                        log.info('metadata not found for datasourceID: ' + dSourceResID)
                        Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                              MessageName='AIS findDataResources error response')
                        Response.error_num = Response.ResponseCodes.NOT_FOUND
                        Response.error_str = "Metadata not found."
                        defer.returnValue(Response)
                    
                else:
                    dSourceResID = yield self.getAssociatedSource(dSetResID)
                    try:
                        dSource = yield self.rc.get_instance(dSourceResID)
                    
                    except ResourceClientError: 
                        log.error('ResourceClientError Exception! Could not get instance for ID: %s' % (dSourceResID))
                        Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                              MessageName='AIS findDataResources error response')
                        Response.error_num = Response.ResponseCodes.NOT_FOUND
                        Response.error_str = "Datasource not found."
                        defer.returnValue(Response)        

                #
                # Added this for Tim and Tom; not sure we need it yet...
                #
                #ownerID = yield self.getAssociatedOwner(dSetResID)
                ownerID = 'Is this used?'

                if typeFlag is self.ALL:
                    #
                    # This was a findDataResources request; the list should only
                    # include datasets that public (so "registered" is not a
                    # problem).
                    #
                    rspMsg.message_parameters_reference[0].dataResourceSummary.add()

                    #
                    # Set the notificationSet flag; this is not efficient at all
                    #
                    if self.bUseMetadataCache:            
                        rspMsg.message_parameters_reference[0].dataResourceSummary[j].notificationSet = self.__isNotificationSet(dSetResID)
                        rspMsg.message_parameters_reference[0].dataResourceSummary[j].date_registered = dSource['registration_datetime_millis']
                    else:
                        rspMsg.message_parameters_reference[0].dataResourceSummary[j].notificationSet = False
                        rspMsg.message_parameters_reference[0].dataResourceSummary[j].date_registered = dSource.registration_datetime_millis
                        
                    self.__loadRspPayload(rspMsg.message_parameters_reference[0].dataResourceSummary[j].datasetMetadata, dSetMetadata, ownerID, dSetResID)
                    
                else:
                    #
                    # This was a findDataResourcesByUser request; do not include
                    # datasets that are registered (in fact, I'm only including
                    # datasets thare are either public or private).
                    #
                    """
                    if ((dSet.ResourceLifeCycleState == dSource.ACTIVE) or
                       (dSet.ResourceLifeCycleState == dSource.COMMISSIONED)):
                        rspMsg.message_parameters_reference[0].datasetByOwnerMetadata.add()
                        self.__loadRspByOwnerPayload(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata[j], dSetMetadata, ownerID, dSet, dSource)
                    """                        

                    rspMsg.message_parameters_reference[0].datasetByOwnerMetadata.add()
                    self.__loadRspByOwnerPayload(rspMsg.message_parameters_reference[0].datasetByOwnerMetadata[j], dSetMetadata, ownerID, dSource)


                #self.__printRootAttributes(dSet)
                #self.__printRootVariables(dSet)
                #self.__printSourceMetadata(dSource)
                #self.__printDownloadURL()
    
                j = j + 1
            else:
                if log.getEffectiveLevel() <= logging.DEBUG:
                    if 'title' in dSetMetadata.keys():
                        log.debug('dataset %s is OUT OF bounds <-------------' % (dSetMetadata['title']))
            
            i = i + 1

        log.debug('__getDataResources exit')


    @defer.inlineCallbacks
    def __findResourcesOfType(self, resourceType, resourceState):

        log.debug('__findResourcesOfType() entry')
        
        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        #
        # Set up a search term using:
        # - TYPE_OF_ID as predicate
        # - object of type: resourceType parameter as object
        #
        pair = request.pairs.add()
    
        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # ..(object)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = resourceType
        pair.object = type_ref

        # 
        # Set up a search term using:
        # - HAS_LIFE_CYCLE_STATE_ID as predicate
        # - LCS_REFERENCE_TYPE object set to given resourceState as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = HAS_LIFE_CYCLE_STATE_ID

        pair.predicate = pref

        # ..(object)
        state_ref = request.CreateObject(LCS_REFERENCE_TYPE)
        if resourceState == self.PRIVATE:
            state_ref.lcs = state_ref.LifeCycleState.ACTIVE
        else:
            state_ref.lcs = state_ref.LifeCycleState.COMMISSIONED
        pair.object = state_ref

        log.debug('Getting resources of type: %s' % (resourceType))

        try:
            result = yield self.asc.get_subjects(request)

        except AssociationServiceError:
            log.error('__findResourcesOfType: association error!')
            defer.returnValue(None)

        log.debug('__findResourcesOfType() exit: found %d resources' % len(result.idrefs))
        
        defer.returnValue(result)

        
    @defer.inlineCallbacks
    def __findResourcesOfTypeAndOwner(self, resourceType, owner):

        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        #
        # Set up a search term using:
        # - OWNED_BY_ID as predicate
        # - LCS_REFERENCE_TYPE object set to ACTIVE as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # ..(object)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = owner
        
        pair.object = type_ref

        #
        # Set up a search term using:
        # - TYPE_OF_ID as predicate
        # - object of type: resourceType parameter as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # ..(object)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = resourceType
        pair.object = type_ref
        
        log.info('Getting resources of type %s with owner: %s' % (resourceType, owner))

        try:
            result = yield self.asc.get_subjects(request)
        
        except AssociationServiceError:
            log.error('__findResourcesOfTypeAndOwner: association error!')
            defer.returnValue(None)
        
        defer.returnValue(result)


    @defer.inlineCallbacks
    def __findPrivateDatasetResourcesByOwner(self, owner):

        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        #
        # Set up a search term using:
        # - TYPE_OF_ID as predicate
        # - object of type: resourceType parameter as object
        #
        pair = request.pairs.add()
    
        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # ..(object)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = DATASET_RESOURCE_TYPE_ID
        pair.object = type_ref

        #
        # Set up a search term using:
        # - OWNED_BY_ID as predicate
        # - LCS_REFERENCE_TYPE object set to ACTIVE as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # ..(object)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = owner
        
        pair.object = type_ref

        # 
        # Set up a search term using:
        # - HAS_LIFE_CYCLE_STATE_ID as predicate
        # - LCS_REFERENCE_TYPE object set to ACTIVE (private) as object
        #
        pair = request.pairs.add()

        # ..(predicate)
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = HAS_LIFE_CYCLE_STATE_ID

        pair.predicate = pref

        # ..(object)
        state_ref = request.CreateObject(LCS_REFERENCE_TYPE)
        state_ref.lcs = state_ref.LifeCycleState.ACTIVE
        pair.object = state_ref

        log.info('Getting private datasets with owner: ' + owner)

        try:
            result = yield self.asc.get_subjects(request)
        
        except AssociationServiceError:
            log.error('__findPrivateResourcesByOwner: association error!')
            defer.returnValue(None)

        log.debug('__findPrivateResourcesByOwner() exit: found %d resources' % len(result.idrefs))
       
        defer.returnValue(result)


    def __loadMinMetaData(self, dSet, dSetMetadata):
        for attrib in dSet.root_group.attributes:
            #log.debug('Root Attribute: %s = %s'  % (str(attrib.name), str(attrib.GetValue())))
            if attrib.name == 'title':
                dSetMetadata['title'] = attrib.GetValue()
            elif attrib.name == 'institution':                
                dSetMetadata['institution'] = attrib.GetValue()
            elif attrib.name == 'source':                
                dSetMetadata['source'] = attrib.GetValue()
            elif attrib.name == 'references':                
                dSetMetadata['references'] = attrib.GetValue()
            elif attrib.name == 'ion_time_coverage_start':                
                dSetMetadata['ion_time_coverage_start'] = attrib.GetValue()
            elif attrib.name == 'ion_time_coverage_end':                
                dSetMetadata['ion_time_coverage_end'] = attrib.GetValue()
            elif attrib.name == 'summary':                
                dSetMetadata['summary'] = attrib.GetValue()
            elif attrib.name == 'comment':                
                dSetMetadata['comment'] = attrib.GetValue()
            elif attrib.name == 'ion_geospatial_lat_min':                
                dSetMetadata['ion_geospatial_lat_min'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_lat_max':                
                dSetMetadata['ion_geospatial_lat_max'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_lon_min':                
                dSetMetadata['ion_geospatial_lon_min'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_lon_max':                
                dSetMetadata['ion_geospatial_lon_max'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_vertical_min':                
                dSetMetadata['ion_geospatial_vertical_min'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_vertical_max':                
                dSetMetadata['ion_geospatial_vertical_max'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_vertical_positive':                
                dSetMetadata['ion_geospatial_vertical_positive'] = attrib.GetValue()


    def __printDownloadURL(self):
        log.debug('Download URL: ' + self.downloadURL)


    def __printRootAttributes(self, ds):
        for atrib in ds.root_group.attributes:
            log.debug('Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue())))


    def __printRootVariables(self, ds):
        for var in ds.root_group.variables:
            log.debug('Root Variable: %s' % str(var.name))
            for atrib in var.attributes:
                log.debug("Attribute: %s = %s" % (str(atrib.name), str(atrib.GetValue())))
            print "....Dimensions:"
            for dim in var.shape:
                log.debug("    ....%s (%s)" % (str(dim.name), str(dim.length)))


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
        
        return self.downloadURL

    def __loadRspByOwnerPayload(self, rspPayload, dSetMetadata, userID, dSource):
        rspPayload.data_resource_id = dSetMetadata['ResourceIdentity']
        rspPayload.title = dSetMetadata['title']
        #rspPayload.date_registered = dSource.registration_datetime_millis
        rspPayload.date_registered = dSource['registration_datetime_millis']
        #rspPayload.ion_title = dSource.ion_title
        rspPayload.ion_title = dSource['ion_title']
        #rspPayload.activation_state = dSource.ResourceLifeCycleState
        #
        # Set the activate state based on the resource lcs
        #
        """
        FIXME FIXME FIXME: there needs to be an element in dSource representing state
        if dSource.ResourceLifeCycleState == dSource.NEW:
            rspPayload.activation_state = self.REGISTERED
        elif dSource.ResourceLifeCycleState == dSource.ACTIVE:
            rspPayload.activation_state = self.PRIVATE
        elif dSource.ResourceLifeCycleState == dSource.COMMISSIONED:
            rspPayload.activation_state = self.PUBLIC
        else:
            rspPayload.activation_state = self.UNKNOWN
        """            
        rspPayload.activation_state = dSource['lcs']
        #rspPayload.update_interval_seconds = dSource.update_interval_seconds
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
        
        

