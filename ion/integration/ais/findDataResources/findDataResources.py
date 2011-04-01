#!/usr/bin/env python

"""
@file ion/integration/ais/findDataResources/findDataResources.py
@author David Everett
@brief Worker class to find resources for a given user id, bounded by
spacial and temporal parameters.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
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
from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import FIND_DATA_RESOURCES_RSP_MSG_TYPE

class FindDataResources(object):
    
    def __init__(self, ais):
        log.info('FindDataResources.__init__()')
        self.ais = ais
        self.rc = ResourceClient()
        self.mc = ais.mc
        self.dscc = DatasetControllerClient()
        self.asc = AssociationServiceClient()

        self.dsID = None

    def setTestDatasetID(self, dsID):
        self.dsID = dsID

    @defer.inlineCallbacks
    def __findResourcesOfType(self, resourceType):

        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = resourceType
        
        pair.object = type_ref

        # Add a life cycle state request
        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = HAS_LIFE_CYCLE_STATE_ID

        pair.predicate = pref


        # Set the Object search term
        state_ref = request.CreateObject(LCS_REFERENCE_TYPE)
        state_ref.lcs = state_ref.LifeCycleState.ACTIVE
        pair.object = state_ref

        result = yield self.asc.get_subjects(request)
        
        defer.returnValue(result)

        
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
        log.debug('Found ' + str(len(dSetResults.idrefs)) + 'datasets.')

        # Get the list of datasource resource IDs
        dSourceResults = yield self.__findResourcesOfType(DATASOURCE_RESOURCE_TYPE_ID)
        log.debug('Found ' + str(len(dSourceResults.idrefs)) + 'datasources.')

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
        while i < len(dSetResults.idrefs):
            dSetResID = dSetResults.idrefs[i].key
            dSourceResID = dSourceResults.idrefs[i].key
            log.debug('DHE: Working on datasetResID: ' + dSetResID + ' and dSourceResID:' + dSourceResID)
            
            dSet = yield self.rc.get_instance(dSetResID)
            dSource = yield self.rc.get_instance(dSourceResID)
    
            self.__printRootAttributes(dSet)
            self.__printRootVariables(dSet)
            self.__printSourceMetadata(dSource)

            rspMsg.message_parameters_reference[0].dataResourceSummary.add()
    
            self.__loadRootAttributes(rspMsg.message_parameters_reference[0].dataResourceSummary[i], dSet, userID, dSetResID)
            
            i = i + 1


        defer.returnValue(rspMsg)

    def __loadBounds(self, bounds, msg):
        """
        Load up the bounds dictionary object with the given spatial and temporal
        parameters.
        """
        log.debug('__loadBounds')
        
        bounds['minLat'] = msg.message_parameters_reference.minLatitude
        bounds['maxLat'] = msg.message_parameters_reference.maxLatitude
        bounds['minLon'] = msg.message_parameters_reference.minLongitude
        bounds['maxLon'] = msg.message_parameters_reference.maxLongitude
        bounds['minVert'] = msg.message_parameters_reference.minVertical
        bounds['maxVert'] = msg.message_parameters_reference.maxVertical
        bounds['posVert'] = msg.message_parameters_reference.posVertical
        bounds['minTime'] = msg.message_parameters_reference.minTime
        bounds['maxTime'] = msg.message_parameters_reference.maxTime

        
    def __isInBounds(self, bounds, dSet):
        """
        Determine if dataset resource is in bounds.
        Input:
          - bounds
          - dSet
        """
        log.debug('__isInBounds')
        
    def __printBounds(self, bounds):
        boundNames = list(bounds)
        log.debug('Spatial and Temporal Bounds: ')
        for boundName in boundNames:
            log.debug('   %s = %s'  % (boundName, bounds[boundName]))
    
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

    def __loadRootAttributes(self, rootAttributes, ds, userID, resID):
        try:
            rootAttributes.user_ooi_id = userID
            rootAttributes.data_resource_id = resID
            rootAttributes.title = ds.root_group.FindAttributeByName('title').GetValue()
            rootAttributes.institution = ds.root_group.FindAttributeByName('institution').GetValue()
            rootAttributes.source = ds.root_group.FindAttributeByName('source').GetValue()
            rootAttributes.references = ds.root_group.FindAttributeByName('references').GetValue()
            rootAttributes.ion_time_coverage_start = ds.root_group.FindAttributeByName('ion_time_coverage_start').GetValue()
            rootAttributes.ion_time_coverage_end = ds.root_group.FindAttributeByName('ion_time_coverage_end').GetValue()
            rootAttributes.summary = ds.root_group.FindAttributeByName('summary').GetValue()
            rootAttributes.comment = ds.root_group.FindAttributeByName('comment').GetValue()
            rootAttributes.ion_geospatial_lat_min = float(ds.root_group.FindAttributeByName('ion_geospatial_lat_min').GetValue())
            rootAttributes.ion_geospatial_lat_max = float(ds.root_group.FindAttributeByName('ion_geospatial_lat_max').GetValue())
            rootAttributes.ion_geospatial_lon_min = float(ds.root_group.FindAttributeByName('ion_geospatial_lon_min').GetValue())
            rootAttributes.ion_geospatial_lon_max = float(ds.root_group.FindAttributeByName('ion_geospatial_lon_max').GetValue())
            rootAttributes.ion_geospatial_vertical_min = float(ds.root_group.FindAttributeByName('ion_geospatial_vertical_min').GetValue())
            rootAttributes.ion_geospatial_vertical_max = float(ds.root_group.FindAttributeByName('ion_geospatial_vertical_max').GetValue())
            rootAttributes.ion_geospatial_vertical_positive = ds.root_group.FindAttributeByName('ion_geospatial_vertical_positive').GetValue()
        
        except:
            estr = 'Object ERROR!'
            log.exception(estr)


    """
    def __loadRootVariable(self, rootVariable, ds, var):
        #lat = ds.root_group.FindVariableByName('lat')
        try:
            rootVariable.standard_name  = var.GetStandardName()
            rootVariable.units = var.GetUnits()
            
        except:            
            estr = 'Object ERROR!'
            log.exception(estr)
    """
