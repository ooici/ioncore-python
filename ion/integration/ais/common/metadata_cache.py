#!/usr/bin/env python

"""
@file ion/integration/ais/common/metadata_cache.py
@author David Everett
@brief Class to cache metadata contained in data sets and data sources.  This
is just an in-memory cache (non-persistent); it uses a dictionary of
dictionaries (multi-dimensional dictionary).  The rows are dictionaries of
either data set metadata for data source metadata; they are indexed by the
resourceID.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from decimal import Decimal

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError
from ion.services.coi.resource_registry.association_client import AssociationClient, AssociationClientError

from ion.services.dm.inventory.association_service import AssociationServiceClient, AssociationServiceError
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, \
    SUBJECT_PREDICATE_QUERY_TYPE, IDREF_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, \
    DATASET_RESOURCE_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID, HAS_A_ID, OWNED_BY_ID

PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)

#
# Data Set Metadata Constants
#
DSET         = 'dset'
DSOURCE      = 'dsource'
KEY          = 'key'
RESOURCE_ID  = 'ResourceIdentity'
DSOURCE_ID   = 'DSourceID'
OWNER_ID     = 'OwnerID'
TITLE        = 'title'
INSTITUTION  = 'institution'
SOURCE       = 'source'
REFERENCES   = 'references'
TIME_START   = 'ion_time_coverage_start'
TIME_END     = 'ion_time_coverage_end'
SUMMARY      = 'summary'
COMMENT      = 'comment'
LAT_MIN      = 'ion_geospatial_lat_min'
LAT_MAX      = 'ion_geospatial_lat_max'
LON_MIN      = 'ion_geospatial_lon_min'
LON_MAX      = 'ion_geospatial_lon_max'
VERT_MIN     = 'ion_geospatial_vertical_min'
VERT_MAX     = 'ion_geospatial_vertical_max'
VERT_POS     = 'ion_geospatial_vertical_positive'

#
# Data Source Metadata Constants
#
REGISTRATION_TIME = 'registration_datetime_millis'
PROPERTY = 'property'
REQUEST_TYPE = 'request_type'
STATION_ID = 'station_id'
BASE_URL = 'base_url'
MAX_INGEST_MILLIS = 'max_ingest_millis'
ION_TITLE = 'ion_title'
LCS = 'lcs'
UPDATE_INTERVAL_SECONDS = 'update_interval_seconds'

class MetadataCache(object):
    
    #
    # these are the mappings from resource life cycle states to the view
    # permission states of a dataset
    # 
    REGISTERED = 'Registered'
    PRIVATE    = 'Private'
    PUBLIC     = 'Public'
    UNKOWNN    = 'Unknown'
    
    __metadata = {}

    def __init__(self, ais):
        log.info('MetadataCache.__init__()')

        self.mc = MessageClient(proc = ais)
        self.asc = AssociationServiceClient(proc = ais)
        self.rc = ResourceClient(proc = ais)
        self.ac = AssociationClient(proc = ais)

        #
        # A lock to ensure exclusive access to cache when updating
        #
        self.cacheLock = {}
        self.cacheLock = defer.DeferredLock()


    @defer.inlineCallbacks
    def loadDataSets(self):
        """
        Find all resources of type DATASET_RESOURCE_TYPE_ID and load their
        metadata.  The private __loadDSetMetadata method will only load
        the metadata if the data set is in the Active (Private) or
        Commissioned (Public) state.
        """

        # Get the list of dataset resource IDs
        dSetResults = yield self.__findResourcesOfType(DATASET_RESOURCE_TYPE_ID)
        if dSetResults == None:
            log.error('Error finding dataset resources.')
            defer.returnValue(False)

        numDSets =  len(dSetResults.idrefs)          
        log.debug('Found ' + str(numDSets) + ' datasets.')

        yield self.__lockCache()
        
        i = 0
        while (i < numDSets):
            yield self.__putDSetMetadata(dSetResults.idrefs[i].key)
            i = i + 1

        self.__unlockCache()
            
        defer.returnValue(True)


    @defer.inlineCallbacks
    def loadDataSources(self):
        """
        Find all resources of type DATASOURCE_RESOURCE_TYPE_ID and load their
        metadata.  The private __loadDSetMetadata method will only load
        the metadata if the data source is in the Active (Private) or
        Commissioned (Public) state.
        """


        
        # Get the list of datasource resource IDs
        dSourceResults = yield self.__findResourcesOfType(DATASOURCE_RESOURCE_TYPE_ID)
        if dSourceResults == None:
            log.error('Error finding datasource resources.')
            defer.returnValue(False)

        numDSources =  len(dSourceResults.idrefs)          
        log.debug('Found ' + str(numDSources) + ' datasources.')

        yield self.__lockCache()
        
        i = 0
        while (i < numDSources):
            yield self.__putDSourceMetadata(dSourceResults.idrefs[i].key)
            i = i + 1

        self.__unlockCache()
            
        defer.returnValue(True)

    @defer.inlineCallbacks
    def getDSet(self, dSetID):
        """
        Get the dictionary entry containing the metadata from the data set
        represented by the given ResourceID (dSetID); return the dataset
        object instead of the metadata.
        """
        
        log.debug('getDSet')

        yield self.__lockCache()
                    
        try:
            metadata = self.__metadata[dSetID]
            log.debug('Metadata keys for ' + dSetID + ': ' + str(metadata.keys()))
            returnValue = metadata[DSET]
        except KeyError:
            log.error('Metadata not found for datasetID: ' + dSetID)
            returnValue = None

        self.__unlockCache()
        
        defer.returnValue(returnValue)


    @defer.inlineCallbacks
    def getDSetMetadata(self, dSetID):
        """
        Get the dictionary entry containing the metadata from the data set
        represented by the given ResourceID (dSetID).
        """
        
        log.debug('getDSetMetadata')

        yield self.__lockCache()
                    
        try:
            metadata = self.__metadata[dSetID]
            log.debug('Metadata keys for ' + dSetID + ': ' + str(metadata.keys()))
            returnValue = metadata
        except KeyError:
            log.error('Metadata not found for datasetID: ' + dSetID)
            returnValue = None

        self.__unlockCache()
        
        defer.returnValue(returnValue)


    @defer.inlineCallbacks
    def putDSetMetadata(self, dSetID):
        """
        Get the instance of the data set represented by the given resource
        ID (dSetID) and call the private __loadDSetMetadata method with the
        data set as an argument. 
        """
        
        log.debug('putDSetMetadata')

        yield self.__lockCache()

        yield self.__putDSetMetadata(dSetID)

        self.__unlockCache()
                    
    
    @defer.inlineCallbacks
    def deleteDSetMetadata(self, dSetID):
        """
        Delete the dictionary entry for the data set represented by the given
        resource ID (dSetID).
        """
        
        log.debug('deleteDSetMetadata')

        yield self.__lockCache()

        #
        # Set the persistent flag to False
        #
        dSetMetadata = self.__metadata[dSetID]
        dSet = dSetMetadata[DSET]
        dSet.Repository.persistent = False

        try:
            self.__metadata.pop(dSetID)
            returnValue = True
        except KeyError:
            log.error('deleteDSetMetadata: datasetID ' + dSetID + ' not cached')
            returnValue = False
                    
        self.__unlockCache()
        
        defer.returnValue(returnValue)

    
    @defer.inlineCallbacks
    def getDSource(self, dSourceID):
        """
        Get the dictionary entry containing the metadata from the data source
        represented by the given ResourceID (dSourceID); return the datasource
        object instead of the metadata
        """
        
        log.debug('getDSource for: ' + dSourceID)
                    
        yield self.__lockCache()

        try:
            metadata = self.__metadata[dSourceID]
            log.debug('Metadata keys for ' + dSourceID + ': ' + str(metadata.keys()))
            returnValue = metadata[DSOURCE]
        except KeyError:
            log.error('Metadata not found for datasetID: ' + dSourceID)
            returnValue = None

        self.__unlockCache()
            
        defer.returnValue(returnValue)            
        

    @defer.inlineCallbacks
    def getDSourceMetadata(self, dSourceID):
        """
        Get the dictionary entry containing the metadata from the data source
        represented by the given ResourceID (dSourceID).
        """
        
        yield self.__lockCache()

        log.debug('getDSourceMetadata')
                    
        try:
            metadata = self.__metadata[dSourceID]
            log.debug('Metadata keys for ' + dSourceID + ': ' + str(metadata.keys()))
            returnValue = metadata
        except KeyError:
            log.error('Metadata not found for datasetID: ' + dSourceID)
            returnValue = None

        self.__unlockCache()
            
        defer.returnValue(returnValue)
    
    
    @defer.inlineCallbacks
    def putDSourceMetadata(self, dSourceID):
        """
        Get the instance of the data source represented by the given resource
        ID (dSourceID) and call the private __loadDSourceMetadata method with the
        data source as an argument. 
        """
        
        log.debug('putDSourceMetadata')

        yield self.__lockCache()

        dSource = yield self.__putDSourceMetadata(dSourceID)

        self.__unlockCache()
                    

    @defer.inlineCallbacks
    def deleteDSourceMetadata(self, dSourceID):
        """
        Delete the dictionary entry for the data source represented by the given
        resource ID (dSourceID).
        """
        
        log.debug('deleteDSourceMetadata')

        yield self.__lockCache()
        
        #
        # Set the persistent flag to False
        #
        dSourceMetadata = self.__metadata[dSourceID]
        dSource = dSourceMetadata[DSOURCE]
        dSource.Repository.persistent = False

        try:
            self.__metadata.pop(dSourceID)
            returnValue = True
        except KeyError:
            log.error('deleteDSourceMetadata: datasourceID ' + dSourceID + ' not cached')
            returnValue = False

        self.__unlockCache()
        
        defer.returnValue(returnValue)


    @defer.inlineCallbacks
    def __lockCache(self):
        """
        Lock the cache to insure exclusive access while updating
        """
        
        log.debug('__lockCache')
        yield self.cacheLock.acquire()


    def __unlockCache(self):
        """
        Lock the cache to insure exclusive access while updating
        """
        
        log.debug('__unlockCache')
        self.cacheLock.release()


    @defer.inlineCallbacks
    def __putDSetMetadata(self, dSetID):
        """
        Get the instance of the data set represented by the given resource
        ID (dSetID) and call the private __loadDSetMetadata method with the
        data set as an argument. 
        """
        
        log.debug('__putDSetMetadata')

        dSet = yield self.rc.get_instance(dSetID)
        yield self.__loadDSetMetadata(dSet)

    
    @defer.inlineCallbacks
    def __putDSourceMetadata(self, dSourceID):
        """
        Get the instance of the data source represented by the given resource
        ID (dSourceID) and call the private __loadDSourceMetadata method with the
        data source as an argument. 
        """
        
        log.debug('__putDSourceMetadata')

        dSource = yield self.rc.get_instance(dSourceID)
        self.__loadDSourceMetadata(dSource)


    @defer.inlineCallbacks
    def __loadDSetMetadata(self, dSet):
        """
        Create and load a dictionary entry with the metadata from the given
        data set, and insert the entry into the __metadata dictionary (a
        dictionary of dictionaries).  Only do this if the data set is Private
        or Public.
        """
        
        #
        # Only cache the metadata if the data set is in the ACTIVE or
        # COMMISSIONED state.
        #
        if ((dSet.ResourceLifeCycleState == dSet.ACTIVE) or 
            (dSet.ResourceLifeCycleState == dSet.COMMISSIONED)):
            dSetMetadata = {}
            #
            # Store the entire dataset now; should be doing only that anyway.
            # Set persisence to true.  NOTE: remember to set this to false
            # on delete.
            #
            dSet.Repository.persistent = True
            dSetMetadata[DSET] = dSet
            dSetMetadata[DSOURCE_ID] = yield self.__getAssociatedSource(dSet)
            dSetMetadata[RESOURCE_ID] = dSet.ResourceIdentity
            dSetMetadata[OWNER_ID] = yield self.__getAssociatedOwner(dSet.ResourceIdentity)
            for attrib in dSet.root_group.attributes:
                #log.debug('Root Attribute: %s = %s'  % (str(attrib.name), str(attrib.GetValue())))
                if attrib.name == TITLE:
                    dSetMetadata[TITLE] = attrib.GetValue()
                elif attrib.name == INSTITUTION:                
                    dSetMetadata[INSTITUTION] = attrib.GetValue()
                elif attrib.name == SOURCE:                
                    dSetMetadata[SOURCE] = attrib.GetValue()
                elif attrib.name == REFERENCES:                
                    dSetMetadata[REFERENCES] = attrib.GetValue()
                elif attrib.name == TIME_START:                
                    dSetMetadata[TIME_START] = attrib.GetValue()
                elif attrib.name == TIME_END:                
                    dSetMetadata[TIME_END] = attrib.GetValue()
                elif attrib.name == SUMMARY:                
                    dSetMetadata[SUMMARY] = attrib.GetValue()
                elif attrib.name == COMMENT:                
                    dSetMetadata[COMMENT] = attrib.GetValue()
                elif attrib.name == LAT_MIN:                
                    dSetMetadata[LAT_MIN] = Decimal(str(attrib.GetValue()))
                elif attrib.name == LAT_MAX:                
                    dSetMetadata[LAT_MAX] = Decimal(str(attrib.GetValue()))
                elif attrib.name == LON_MIN:                
                    dSetMetadata[LON_MIN] = Decimal(str(attrib.GetValue()))
                elif attrib.name == LON_MAX:                
                    dSetMetadata[LON_MAX] = Decimal(str(attrib.GetValue()))
                elif attrib.name == VERT_MIN:                
                    dSetMetadata[VERT_MIN] = Decimal(str(attrib.GetValue()))
                elif attrib.name == VERT_MAX:                
                    dSetMetadata[VERT_MAX] = Decimal(str(attrib.GetValue()))
                elif attrib.name == VERT_POS:                
                    dSetMetadata[VERT_POS] = attrib.GetValue()
            if dSet.ResourceLifeCycleState == dSet.ACTIVE:
                dSetMetadata[LCS] = self.PRIVATE
            elif dSet.ResourceLifeCycleState == dSet.COMMISSIONED:
                dSetMetadata[LCS] = self.PUBLIC
            
            log.debug('dSetMetadata keys: ' + str(dSetMetadata.keys()))
            #
            # Store this dSetMetadata in the dictionary, indexed by the resourceID
            #
            self.__metadata[dSet.ResourceIdentity] = dSetMetadata
    
            self.__printMetadata(dSet)
        else:
            log.info('data set ' + dSet.ResourceIdentity + ' is not Private or Public.')


    def __loadDSourceMetadata(self, dSource):
        """
        Create and load a dictionary entry with the metadata from the given
        data source, and insert the entry into the __metadata dictionary (a
        dictionary of dictionaries).  Only do this if the data source is Private
        or Public.
        """
        
        #
        # Only cache the metadata if the data source is in the ACTIVE or
        # COMMISSIONED state.
        #
        if ((dSource.ResourceLifeCycleState == dSource.ACTIVE) or 
            (dSource.ResourceLifeCycleState == dSource.COMMISSIONED)):
            dSourceMetadata = {}
            #
            # Store the entire datasource now; should be doing only that anyway
            # Set persisence to true.  NOTE: remember to set this to false
            # on delete.
            #
            dSource.Repository.persistent = True
            dSourceMetadata[DSOURCE] = dSource
            for property in dSource.property:
                dSourceMetadata[PROPERTY] = property
    
            for sid in dSource.station_id:                
                dSourceMetadata[STATION_ID] = sid
                
            dSourceMetadata[REGISTRATION_TIME] = dSource.registration_datetime_millis
            dSourceMetadata[REQUEST_TYPE] = dSource.request_type
            dSourceMetadata[BASE_URL] = dSource.base_url
            dSourceMetadata[MAX_INGEST_MILLIS] = dSource.max_ingest_millis
            dSourceMetadata[ION_TITLE] = dSource.ion_title
            dSourceMetadata[UPDATE_INTERVAL_SECONDS] = dSource.update_interval_seconds
            if dSource.ResourceLifeCycleState == dSource.ACTIVE:
                dSourceMetadata[LCS] = self.PRIVATE
            elif dSource.ResourceLifeCycleState == dSource.COMMISSIONED:
                dSourceMetadata[LCS] = self.PUBLIC
            
            log.debug('dSourceMetadata keys: ' + str(dSourceMetadata.keys()))
            #
            # Store this dSourceMetadata in the dictionary, indexed by the resourceID
            #
            self.__metadata[dSource.ResourceIdentity] = dSourceMetadata
    
            self.__printMetadata(dSource)
        else:
            log.info('data source ' + dSource.ResourceIdentity + ' is not Private or Public.')


    @defer.inlineCallbacks
    def __findResourcesOfType(self, resourceType):
        """
        A utility method to find all resources of the given type (resourceType).
        """

        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        #
        # Set up a resource type search term using:
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

        try:
            result = yield self.asc.get_subjects(request)

        except AssociationServiceError:
            log.error('__findResourcesOfType: association error!')
            defer.returnValue(None)

        defer.returnValue(result)

    @defer.inlineCallbacks
    def __getAssociatedSource(self, dSet):
        """
        Worker class method to get the data source that associated with a given
        data set.  
        """
        log.debug('__getAssociatedSource() entry')

        try:
            results = yield self.ac.find_associations(obj=dSet, predicate_or_predicates=HAS_A_ID)

        except AssociationClientError:
            log.error('AssociationError')
            defer.returnValue(None)

        for association in results:
            log.debug('Associated Source for Dataset: ' + \
                      association.ObjectReference.key + \
                      ' is: ' + association.SubjectReference.key)

        log.debug('__getAssociatedSource() exit')

        defer.returnValue(association.SubjectReference.key)


    @defer.inlineCallbacks
    def __getAssociatedOwner(self, dsID):
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


    def __printMetadata(self, res):
        log.debug('Metadata for ' + res.ResourceIdentity + ':')
        for key in self.__metadata[res.ResourceIdentity].keys():
            log.debug('key: ' + key)
        for value in self.__metadata[res.ResourceIdentity].values():
            log.debug('value: ' + str(value))

    def __printObject(self, object):
        log.debug('Object: ' + str(object))
