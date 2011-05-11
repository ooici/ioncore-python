#!/usr/bin/env python

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from decimal import Decimal

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError

from ion.services.dm.inventory.association_service import AssociationServiceClient, AssociationServiceError
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, \
            DATASET_RESOURCE_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID

PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)


KEY          = 'key'
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

class MetadataCache(object):
    
    __metadata = {}

    def __init__(self, ais):
        log.info('MetadataCache.__init__()')

        self.mc = MessageClient(proc = ais)
        self.asc = AssociationServiceClient(proc = ais)
        self.rc = ResourceClient(proc = ais)

        """
        self.ais = ais
        #self.dscc = DatasetControllerClient(proc=ais)
        self.ac = AssociationClient(proc=ais)
        """

    @defer.inlineCallbacks
    def loadDataSets(self):
        
        # Get the list of dataset resource IDs
        dSetResults = yield self.__findResourcesOfType(DATASET_RESOURCE_TYPE_ID)
        if dSetResults == None:
            log.error('Error finding resources.')
            defer.returnValue(False)

        numDSets =  len(dSetResults.idrefs)          
        log.debug('Found ' + str(numDSets) + ' datasets.')

        i = 0
        while (i < numDSets):
            dSet = yield self.rc.get_instance(dSetResults.idrefs[i])
            self.__loaddSetMetadata(dSet)
            i = i + 1
            
        defer.returnValue(True)


    def getMetadata(self, dSetID):
        log.debug('getMetadata')
                    
        try:
            metadata = self.__metadata[dSetID]
            log.debug('Metadata keys for ' + dSetID + ': ' + str(metadata.keys()))
        except KeyError:
            log.error('Metadata not found for datasetID: ' + dSetID)
            return None
        
        return metadata
    
    
    def __loaddSetMetadata(self, dSet):
        dSetMetadata = {}
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
        
        log.debug('keys: ' + str(dSetMetadata.keys()))
        #
        # Store this dSetMetadata in the dictionary, indexed by the resourceID
        #
        self.__metadata[dSet.ResourceIdentity] = dSetMetadata

        self.__printMetadata(dSet)


    def __printMetadata(self, dSet):
        log.debug('Metadata for ' + dSet.ResourceIdentity + ':')
        for key in self.__metadata[dSet.ResourceIdentity].keys():
            log.debug('key: ' + key)
        for value in self.__metadata[dSet.ResourceIdentity].values():
            log.debug('value: ' + str(value))

        
    @defer.inlineCallbacks
    def __findResourcesOfType(self, resourceType):

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

    