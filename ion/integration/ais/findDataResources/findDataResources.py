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
from ion.integration.ais.ais_object_identifiers import AIS_DATA_RESOURCE_SUMMARY_MSG_TYPE

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
        
        #defer.returnValue(resID)
        defer.returnValue(result)

        
    @defer.inlineCallbacks
    def findDataResources(self, msg):
        log.debug('findDataResources Worker Class')

        """
        Need to build up a GPB Message;
         - get request message object from message client
         - build up request message to dataset_controller based on incoming
           request message
         - send to dataset_controller client to get list of resource ids
         - get results
         - for each resource id: determine if in bounds
         - use cdm dataset helper methods (resource_client) to get pertinent
           metadata; determine if in bounds of spatial/temporal parms.
         - get response message object from message client
         - build up response message
         - get response message payload
         - build up response message payload
         - return response message to ais service
        """

        
        """
        This is currently a call to a stub; the msg contains the actual resource
        id.
        """

        userID = msg.message_parameters_reference.user_ooi_id        

        #### TEST TEST TEST
        #datasetResID = self.ais.getTestDatasetID()
        result = yield self.__findResourcesOfType(DATASET_RESOURCE_TYPE_ID)
        datasetResID = result.idrefs[0].key


        log.debug('DHE: Stub find_data_resources returned datasetResID: ' + datasetResID)
        
        log.debug('DHE: findDataResources getting resource instance')
        ds = yield self.rc.get_instance(datasetResID)
        #log.debug('DHE: get_instance returned ' + str(ds))

        """
        I think this should print out everything in the dataset
        """
        for atrib in ds.root_group.attributes:
            print 'Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue()))

        for var in ds.root_group.variables:
            print 'Root Variable: %s' % str(var.name)
            for atrib in var.attributes:
                print "Attribute: %s = %s" % (str(atrib.name), str(atrib.GetValue()))
            print "....Dimensions:"
            for dim in var.shape:
                print "    ....%s (%s)" % (str(dim.name), str(dim.length))
        
        #dimensions    =  [str(dim.name) for dim in lat.shape]
 
        """
        lat = ds.root_group.FindAttributeByName('latitude')
        log.debug("Here are the latitude values")
        for value in lat.GetValues():
            log.debug(str(value))
        """

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_RSP_MSG_TYPE)
        rspMsg.message_parameters_reference[0].dataResourceSummary.add()
        rspMsg.message_parameters_reference[0].dataResourceSummary[0] = \
            rspMsg.CreateObject(AIS_DATA_RESOURCE_SUMMARY_MSG_TYPE)

        self.__loadRootAttributes(rspMsg.message_parameters_reference[0].dataResourceSummary[0], ds, userID, datasetResID)

        """
        Now get the datasource resource
        """
        
        result = yield self.__findResourcesOfType(DATASOURCE_RESOURCE_TYPE_ID)
        datasourceResID = result.idrefs[0].key
        log.debug('DHE: Stub find_data_resources returned datasourceResID: ' + datasourceResID)

        """
        Moving this to getDataResourceDetail
        i = 0
        for var in ds.root_group.variables:
            print 'Working on variable: %s' % str(var.name)
            rspMsg.message_parameters_reference[0].dataResourceSummary[0].variable.add()
            self.__loadRootVariable(rspMsg.message_parameters_reference[0].dataResourceSummary[0].variable[i], ds, var)
            i = i + 1
        """            

        defer.returnValue(rspMsg)

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
