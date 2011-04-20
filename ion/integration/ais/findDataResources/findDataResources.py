#!/usr/bin/env python

"""
@file ion/integration/ais/findDataResources/findDataResources.py
@author David Everett
@brief Worker class to find resources for a given user id, bounded by
spacial and temporal parameters.
"""

import time, datetime
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from decimal import Decimal

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
from ion.services.coi.resource_registry_beta.association_client import AssociationClient, AssociationInstance, AssociationManager
from ion.services.coi.resource_registry_beta.association_client import AssociationClientError
#from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
# DHE Temporarily pulling DatasetControllerClient from scaffolding
from ion.integration.ais.findDataResources.resourceStubs import DatasetControllerClient
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, HAS_A_ID, IDENTITY_RESOURCE_TYPE_ID, TYPE_OF_ID, ANONYMOUS_USER_ID, HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, \
            SAMPLE_PROFILE_DATASET_ID, DATASET_RESOURCE_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID


from ion.core.object import object_utils
ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
LCS_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=26, version=1)

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE
from ion.integration.ais.ais_object_identifiers import FIND_DATA_RESOURCES_RSP_MSG_TYPE

DNLD_BASE_THREDDS_URL = 'http://localhost:8081/thredds'
DNLD_DIR_PATH = '/dodsC/scanData/'
DNLD_FILE_TYPE = '.ncml'

NORTHERN_HEMISPHERE = 'nothern'
SOUTHERN_HEMISPHERE = 'southern'
WESTERN_HEMISPHERE = 'western'
EASTERN_HEMISPHERE = 'eastern'

class FindDataResources(object):
    
    def __init__(self, ais):
        log.info('FindDataResources.__init__()')
        self.ais = ais
        self.rc = ResourceClient()
        self.mc = ais.mc
        self.dscc = DatasetControllerClient()
        self.asc = AssociationServiceClient()
        self.ac = AssociationClient()


        self.dsID = None

    def setTestDatasetID(self, dsID):
        self.dsID = dsID

    @defer.inlineCallbacks
    def findDataResources(self, msg):
        """
        Worker class method called by app_integration_service to implement
        findDataResources.  Finds all dataset resources that are "published"
        and returns their IDs along with a load of metadata.
        """

        log.debug('findDataResources Worker Class Method')

        #
        # I don't think this is needed, but leaving it in for now
        #
        userID = msg.message_parameters_reference.user_ooi_id

        self.downloadURL       = 'Uninitialized'
        self.filterByLatitude  = True
        self.filterByLongitude = True
        self.filterByVertical  = True
        self.filterByTime      = True
        self.bIsInAreaBounds      = True
        self.bIsInVerticalBounds  = True
        self.bIsInTimeBounds      = True

        bounds = {}
        self.__loadBounds(bounds, msg)
        self.__printBounds(bounds)
        
        #
        # Create the response message to which we will attach the list of
        # resource IDs
        #
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_RSP_MSG_TYPE)

        # Get the list of dataset resource IDs
        dSetResults = yield self.__findResourcesOfType(DATASET_RESOURCE_TYPE_ID)
        log.debug('Found ' + str(len(dSetResults.idrefs)) + ' datasets.')

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
        while i < len(dSetResults.idrefs):
            dSetResID = dSetResults.idrefs[i].key
            log.debug('Working on dataset: ' + dSetResID)
            
            dSet = yield self.rc.get_instance(dSetResID)

            minMetaData = {}
            self.__loadMinMetaData(dSet, minMetaData)

            #
            # If the dataset's data is within the given criteria, include it
            # in the list
            #
            if self.filterByLatitude:
                self.bIsInAreaBounds = self.__isInLatitudeBounds(minMetaData, bounds)

            if self.bIsInAreaBounds and self.filterByLongitude:
                self.bIsInAreaBounds = self.__isInLongitudeBounds(minMetaData, bounds)

            if self.bIsInAreaBounds and self.filterByVertical:
                self.bIsInVerticalBounds = self.__isInVerticalBounds(minMetaData, bounds)
                                    
            if self.bIsInAreaBounds and self.bIsInVerticalBounds and self.filterByTime:
                self.bIsInTimeBounds = self.__isInTimeBounds(minMetaData, bounds)

            if self.bIsInAreaBounds and self.bIsInTimeBounds and self.bIsInVerticalBounds:

                dSourceResID = yield self.getAssociatedSource(dSetResID)
                dSource = yield self.rc.get_instance(dSourceResID)
                
                rspMsg.message_parameters_reference[0].dataResourceSummary.add()

                self.__createDownloadURL(dSetResID)
                self.__loadRootAttributes(rspMsg.message_parameters_reference[0].dataResourceSummary[j], minMetaData, userID, dSetResID)

                self.__printRootAttributes(dSet)
                self.__printRootVariables(dSet)
                self.__printSourceMetadata(dSource)
                self.__printDownloadURL()
    
                j = j + 1
            else:
                log.debug("isInBounds is FALSE")

            
            i = i + 1


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
        self.filterByLatitude  = True
        self.filterByLongitude = True
        self.filterByVertical = True
        self.filterByTime     = True
        self.bIsInAreaBounds      = True
        self.bIsInVerticalBounds  = True
        self.bIsInTimeBounds      = True
        
        bounds = {}
        self.__loadBounds(bounds, msg)
        self.__printBounds(bounds)
        
        #
        # Create the response message to which we will attach the list of
        # resource IDs
        #
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_RSP_MSG_TYPE)

        # Get the list of dataset resource IDs
        dSetResults = yield self.__findResourcesOfTypeAndOwner(DATASET_RESOURCE_TYPE_ID, userID)
        log.debug('Found ' + str(len(dSetResults.idrefs)) + ' datasets.')

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
        while i < len(dSetResults.idrefs):
            dSetResID = dSetResults.idrefs[i].key
            log.debug('DHE: Working on datasetResID: ' + dSetResID)
            
            dSet = yield self.rc.get_instance(dSetResID)

            minMetaData = {}
            self.__loadMinMetaData(dSet, minMetaData)

            #
            # If the dataset's data is within the given criteria, include it
            # in the list
            #
            if self.filterByLatitude:
                self.bIsInAreaBounds = self.__isInLatitudeBounds(minMetaData, bounds)

            if self.bIsInAreaBounds and self.filterByLongitude:
                self.bIsInAreaBounds = self.__isInLongitudeBounds(minMetaData, bounds)

            if self.bIsInAreaBounds and self.filterByVertical:
                self.bIsInVerticalBounds = self.__isInVerticalBounds(minMetaData, bounds)
                                    
            if self.bIsInAreaBounds and self.bIsInVerticalBounds and self.filterByTime:
                self.bIsInTimeBounds = self.__isInTimeBounds(minMetaData, bounds)

            if self.bIsInAreaBounds and self.bIsInTimeBounds and self.bIsInVerticalBounds:

                dSourceResID = yield self.getAssociatedSource(dSetResID)
                dSource = yield self.rc.get_instance(dSourceResID)
                
                rspMsg.message_parameters_reference[0].dataResourceSummary.add()

                self.__createDownloadURL(dSetResID)
                self.__loadRootAttributes(rspMsg.message_parameters_reference[0].dataResourceSummary[j], minMetaData, userID, dSetResID)

                self.__printRootAttributes(dSet)
                self.__printRootVariables(dSet)
                self.__printSourceMetadata(dSource)
                self.__printDownloadURL()
    
                j = j + 1
            else:
                log.debug("isInBounds is FALSE")

            
            i = i + 1


        defer.returnValue(rspMsg)


    @defer.inlineCallbacks
    def getAssociatedSource(self, dSetResID):
        """
        Worker class method to get the data source that associated with a given
        data set.  This is a public method because it can be called from the
        findDataResourceDetail service.
        """
        log.debug('getAssociatedSource()')

        ds = yield self.rc.get_instance(dSetResID)

        results = yield self.ac.find_associations(obj=ds, predicate_or_predicates=HAS_A_ID)
        for association in results:
            log.debug('Associated Source for Dataset: ' + \
                      association.ObjectReference.key + \
                      ' is: ' + association.SubjectReference.key)

        defer.returnValue(association.SubjectReference.key)
                      


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

        # 
        # Set up a life cycle state term using:
        # - HAS_LIFE_CYCLE_STATE_ID as predicate
        # - LCS_REFERENCE_TYPE object set to ACTIVE as object
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

        result = yield self.asc.get_subjects(request)
        
        defer.returnValue(result)

        
    @defer.inlineCallbacks
    def __findResourcesOfTypeAndOwner(self, resourceType, owner):

        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

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

        # ..(object)
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = owner
        
        pair.object = type_ref

        #
        # Set up an owned_by_id search term using:
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

        result = yield self.asc.get_subjects(request)
        
        defer.returnValue(result)


    def __loadMinMetaData(self, dSet, minMetaData):
        for attrib in dSet.root_group.attributes:
            log.debug('Root Attribute: %s = %s'  % (str(attrib.name), str(attrib.GetValue())))
            if attrib.name == 'title':
                minMetaData['title'] = attrib.GetValue()
            elif attrib.name == 'institution':                
                minMetaData['institution'] = attrib.GetValue()
            elif attrib.name == 'source':                
                minMetaData['source'] = attrib.GetValue()
            elif attrib.name == 'references':                
                minMetaData['references'] = attrib.GetValue()
            elif attrib.name == 'ion_time_coverage_start':                
                minMetaData['ion_time_coverage_start'] = attrib.GetValue()
            elif attrib.name == 'ion_time_coverage_end':                
                minMetaData['ion_time_coverage_end'] = attrib.GetValue()
            elif attrib.name == 'summary':                
                minMetaData['summary'] = attrib.GetValue()
            elif attrib.name == 'comment':                
                minMetaData['comment'] = attrib.GetValue()
            elif attrib.name == 'ion_geospatial_lat_min':                
                minMetaData['ion_geospatial_lat_min'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_lat_max':                
                minMetaData['ion_geospatial_lat_max'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_lon_min':                
                minMetaData['ion_geospatial_lon_min'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_lon_max':                
                minMetaData['ion_geospatial_lon_max'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_vertical_min':                
                minMetaData['ion_geospatial_vertical_min'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_vertical_max':                
                minMetaData['ion_geospatial_vertical_max'] = Decimal(str(attrib.GetValue()))
            elif attrib.name == 'ion_geospatial_vertical_positive':                
                minMetaData['ion_geospatial_vertical_positive'] = attrib.GetValue()


    def __loadTimeBounds(self, bounds, msg):
        log.debug('__loadTimeBounds')

        
    def __loadBounds(self, bounds, msg):
        """
        Load up the bounds dictionary object with the given spatial and temporal
        parameters.
        """
        log.debug('__loadBounds')
        
        #
        # Determine if only one of the latitude bounds have been set; if so,
        # use the other to determine whether which hemisphere.  If no latitude
        # bounds have been given, set filterByLatitude to False.
        #
        
        bIsMinLatitudeSet =  msg.message_parameters_reference.IsFieldSet('minLatitude')
        bIsMaxLatitudeSet =  msg.message_parameters_reference.IsFieldSet('maxLatitude')
        bIsMinLongitudeSet =  msg.message_parameters_reference.IsFieldSet('minLongitude')
        bIsMaxLongitudeSet =  msg.message_parameters_reference.IsFieldSet('maxLongitude')
        

        if bIsMinLatitudeSet:
            bounds['minLat'] = Decimal(str(msg.message_parameters_reference.minLatitude))
        if bIsMaxLatitudeSet:
            bounds['maxLat'] = Decimal(str(msg.message_parameters_reference.maxLatitude))
        if bIsMinLongitudeSet:
            bounds['minLon'] = Decimal(str(msg.message_parameters_reference.minLongitude))
        if bIsMaxLongitudeSet:
            bounds['maxLon'] = Decimal(str(msg.message_parameters_reference.maxLongitude))


        #
        # If both minLat and maxLat are not set, we need to determine which
        # hemisphere the other is in (if it's set), so that we can know how
        # to default the one that isn't set.  If none are set we won't filter
        # by area.  Same with minLon and maxLon.
        #
        if not (bIsMinLatitudeSet and bIsMaxLatitudeSet): 
            if bIsMinLatitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                if bounds['minLat'] >= 0:
                    bounds['maxLat'] = Decimal('90')
                    latHemisphere = NORTHERN_HEMISPHERE
                else:
                    bounds['maxLat'] = Decimal('0')
                    latHemisphere = SOUTHERN_HEMISPHERE
            elif bIsMaxLatitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                if bounds['maxLat'] >= 0:
                    bounds['minLat'] = Decimal('0')
                    latHemisphere = NORTHERN_HEMISPHERE
                else:
                    bounds['minLat'] = Decimal('-90')
                    latHemisphere = SOUTHERN_HEMISPHERE
            else:
                self.filterByLatitude = False

        if not (bIsMinLongitudeSet and bIsMaxLongitudeSet): 
            if bIsMinLongitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                if bounds['minLon'] >= 0:
                    bounds['maxLon'] = Decimal('180')
                    lonHemisphere = EASTERN_HEMISPHERE
                else:
                    bounds['maxLon'] = Decimal('0')
                    lonHemisphere = WESTERN_HEMISPHERE
            elif bIsMaxLongitudeSet:
                #
                # Determine whether northern or southern hemisphere
                #
                if bounds['maxLon'] >= 0:
                    bounds['minLon'] = Decimal('0')
                    lonHemisphere = EASTERN_HEMISPHERE
                else:
                    bounds['minLon'] = Decimal('-180')
                    lonHemisphere = WESTERN_HEMISPHERE
            else:
                self.filterByLongitude = False


        if msg.message_parameters_reference.IsFieldSet('minVertical'):
            bounds['minVert'] = Decimal(str(msg.message_parameters_reference.minVertical))
        else:
            self.filterByVertical = False

        if msg.message_parameters_reference.IsFieldSet('maxVertical'):
            bounds['maxVert'] = Decimal(str(msg.message_parameters_reference.maxVertical))
        else:
            self.filterByVertical = False

        if msg.message_parameters_reference.IsFieldSet('posVertical'):
            bounds['posVert'] = msg.message_parameters_reference.posVertical
        else:
            self.filterByVertical = False

        if msg.message_parameters_reference.IsFieldSet('minTime'):
            tmpTime = datetime.datetime.strptime(msg.message_parameters_reference.minTime, \
                                                           '%Y-%m-%dT%H:%M:%SZ')
            bounds['minTime'] = time.mktime(tmpTime.timetuple())
        else:
            self.filterByTime = False

        if msg.message_parameters_reference.IsFieldSet('maxTime'):
            tmpTime = datetime.datetime.strptime(msg.message_parameters_reference.maxTime, \
                                                           '%Y-%m-%dT%H:%M:%SZ')
            bounds['maxTime'] = time.mktime(tmpTime.timetuple())
        else:
            self.filterByTime = False


    def __isInLatitudeBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in latitude bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInLatitudeBounds()')

        if minMetaData['ion_geospatial_lat_min'] < bounds['minLat']:
            log.debug(' %f is < bounds %f' % (minMetaData['ion_geospatial_lat_min'], bounds['minLat']))
            return False
            
        if minMetaData['ion_geospatial_lat_max'] > bounds['maxLat']:
            log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_lat_max'], bounds['maxLat']))
            return False
            
        return True


    def __isInLongitudeBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in longitude bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInLongitudeBounds()')

        if minMetaData['ion_geospatial_lon_min'] < bounds['minLon']:
            log.debug('%s is < bounds %s' % (minMetaData['ion_geospatial_lon_min'], bounds['minLon']))
            return False
            
        if minMetaData['ion_geospatial_lon_max'] > bounds['maxLon']:
            log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_lon_max'], bounds['maxLon']))
            return False
        
        return True


    def __isInVerticalBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in vertical bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInVerticalBounds()')

        if minMetaData['ion_geospatial_vertical_min'] > bounds['minVert']:
            log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_vertical_min'], bounds['minVert']))
            return False
            
        if minMetaData['ion_geospatial_vertical_max'] > bounds['maxVert']:
            log.debug('%s is > bounds %s' % (minMetaData['ion_geospatial_vertical_max'], bounds['maxVert']))
            return False
        
        return True

        
    def __isInTimeBounds(self, minMetaData, bounds):
        """
        Determine if dataset resource is in time bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInTimeBounds()')
        
        """
        try:
            boundMinTime = time.mktime(bounds['minTime'].timetuple())
            print boundMinTime
            
        except ValueError:
            log.error('datetime.strptime did not work!!!!!')
        """
            
        tmpTime = datetime.datetime.strptime(minMetaData['ion_time_coverage_start'], '%Y-%m-%dT%H:%M:%SZ')
        dataMinTime = time.mktime(tmpTime.timetuple())
        if dataMinTime < bounds['minTime']:
            log.debug(' %s is < bounds %s' % (dataMinTime, bounds['minTime']))
            return False
            
        tmpTime = datetime.datetime.strptime(minMetaData['ion_time_coverage_end'], '%Y-%m-%dT%H:%M:%SZ')
        dataMaxTime = time.mktime(tmpTime.timetuple())
        if dataMaxTime > bounds['maxTime']:
            log.debug('%s is > bounds %s' % (dataMaxTime, bounds['maxTime']))
            return False
            
        return True

        
    def __printBounds(self, bounds):
        boundNames = list(bounds)
        log.debug('Spatial and Temporal Bounds: ')
        for boundName in boundNames:
            log.debug('   %s = %s'  % (boundName, bounds[boundName]))


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
        log.debug('source_type: ' + str(dSource.source_type))
        for property in dSource.property:
            log.debug('Property: ' + property)
        for sid in dSource.station_id:
            log.debug('Station ID: ' + sid)
        log.debug('request_type: ' + str(dSource.request_type))
        log.debug('base_url: ' + dSource.base_url)
        log.debug('max_ingest_millis: ' + str(dSource.max_ingest_millis))


    def __loadRootAttributes(self, rootAttributes, minMetaData, userID, dSetResID):
        rootAttributes.user_ooi_id = userID
        rootAttributes.data_resource_id = dSetResID
        rootAttributes.download_url = self.__createDownloadURL(dSetResID)
        for attrib in minMetaData:
            log.debug('Root Attribute: %s = %s'  % (attrib, minMetaData[attrib]))
            if  attrib == 'title':
                rootAttributes.title = minMetaData[attrib]
            elif attrib == 'institution':                
                rootAttributes.institution = minMetaData[attrib]
            elif attrib == 'source':                
                rootAttributes.source = minMetaData[attrib]
            elif attrib == 'references':                
                rootAttributes.references = minMetaData[attrib]
            elif attrib == 'ion_time_coverage_start':                
                rootAttributes.ion_time_coverage_start = minMetaData[attrib]
            elif attrib == 'ion_time_coverage_end':                
                rootAttributes.ion_time_coverage_end = minMetaData[attrib]
            elif attrib == 'summary':                
                rootAttributes.summary = minMetaData[attrib]
            elif attrib == 'comment':                
                rootAttributes.comment = minMetaData[attrib]
            elif attrib == 'ion_geospatial_lat_min':                
                rootAttributes.ion_geospatial_lat_min = float(minMetaData[attrib])
            elif attrib == 'ion_geospatial_lat_max':                
                rootAttributes.ion_geospatial_lat_max = float(minMetaData[attrib])
            elif attrib == 'ion_geospatial_lon_min':                
                rootAttributes.ion_geospatial_lon_min = float(minMetaData[attrib])
            elif attrib == 'ion_geospatial_lon_max':                
                rootAttributes.ion_geospatial_lon_max = float(minMetaData[attrib])
            elif attrib == 'ion_geospatial_vertical_min':                
                rootAttributes.ion_geospatial_vertical_min = float(minMetaData[attrib])
            elif attrib == 'ion_geospatial_vertical_max':                
                rootAttributes.ion_geospatial_vertical_max = float(minMetaData[attrib])
            elif attrib == 'ion_geospatial_vertical_positive':                
                rootAttributes.ion_geospatial_vertical_positive = minMetaData[attrib]


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




