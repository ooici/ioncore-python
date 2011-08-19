#!/usr/bin/env python

"""
@file ion/integration/ais/getDataResourceDetail/getDataResourceDetail.py
@author David Everett
@brief Worker class to get the resource metadata for a given data resource
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import logging
from twisted.internet import defer

from ion.core.object import object_utils
from ion.core.exception import ReceivedApplicationError
from ion.services.coi.identity_registry import IdentityRegistryClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.integration.ais.findDataResources.findDataResources import FindDataResources

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE

from ion.integration.ais.ais_object_identifiers import GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE

USER_OOIID_TYPE = object_utils.create_type_identifier(object_id=1403, version=1)
RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)

class GetDataResourceDetail(object):
    
    def __init__(self, ais):
        log.info('GetDataResourceDetail.__init__()')
        self.ais = ais
        self.rc = ResourceClient(proc=ais)
        self.mc = ais.mc
        self.irc = IdentityRegistryClient(proc=ais)
        self.metadataCache = ais.getMetadataCache()

        
    @defer.inlineCallbacks
    def getDataResourceDetail(self, msg):
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('AIS.getDataResourceDetail()')

        # Check for correct request protocol buffer type
        if msg.MessageType != AIS_REQUEST_MSG_TYPE:
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = 'AIS.getDataResourceDetail: Bad message type receieved, ignoring'
            defer.returnValue(Response)
 
        # Check payload in message
        if not msg.IsFieldSet('message_parameters_reference'):
           # build AIS error response
           Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
           Response.error_num = Response.ResponseCodes.BAD_REQUEST
           Response.error_str = "AIS.getDataResourceDetail: Required field [message_parameters_reference] not found in message"
           defer.returnValue(Response)
           
        if msg.message_parameters_reference.IsFieldSet('data_resource_id'):
            dSetResID = msg.message_parameters_reference.data_resource_id
        else:
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS getDataResourceDetail error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "AIS.getDataResourceDetail: Required field [data_resource_id] not found in message"
            defer.returnValue(Response)

        ds = yield self.metadataCache.getDSet(dSetResID)
        if ds is None:
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS getDataResourceDetail error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "AIS.getDataResourceDetail: No Data Set Found for Dataset ID: " + dSetResID
            defer.returnValue(Response)

        dSetMetadata = yield self.metadataCache.getDSetMetadata(dSetResID)
        dSourceResID = dSetMetadata['DSourceID']
        ownerID = dSetMetadata['OwnerID']

        log.debug('ownerID: ' + ownerID + ' owns dataSetID: ' + dSetResID)
        userProfile = yield self.__getUserProfile(ownerID)

        if (userProfile is None):
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS getDataResourceDetail error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "AIS.getDataResourceDetail: No entry in IR for ownerID " + ownerID
            defer.returnValue(Response)

        if (dSourceResID is None):
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS getDataResourceDetail error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "AIS.getDataResourceDetail: No Associated Data Source Found"
            defer.returnValue(Response)

        log.debug('Associated datasourceID: ' + dSourceResID)
        log.debug('getting cached dSource')
        dSource = yield self.metadataCache.getDSource(dSourceResID)
        if (dSource is None):
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS getDataResourceDetail error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "AIS.getDataResourceDetail: Data Source Found for ID: " + dSourceResID
            defer.returnValue(Response)

        #self.__printSourceMetadata(dSource)
        
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE)

        rspMsg.message_parameters_reference[0].data_resource_id = dSetResID

        self.__loadGPBMinMetaData(rspMsg.message_parameters_reference[0], ds)
        self.__loadGPBSourceMetaData(rspMsg.message_parameters_reference[0].source, dSource, userProfile)
        
        i = 0
        for var in ds.root_group.variables:
            log.debug('Working on variable: ' + str(var.name))
            rspMsg.message_parameters_reference[0].variable.add()
            self.__loadGPBVariable(rspMsg.message_parameters_reference[0].variable[i], ds, var)
            i = i + 1

        defer.returnValue(rspMsg)


    def __loadGPBVariable(self, gpbVariable, ds, var):

        gpbVariable.name = var.name

        #
        # Load up the "minimum metadata" portion of the variable, and then
        # stick any "other_attributes" into a name/value GPB.
        #
        i = 0
        for attrib in var.attributes:
            if attrib.name == 'standard_name':
                gpbVariable.standard_name = attrib.GetValue()
            elif attrib.name == 'long_name':
                gpbVariable.long_name = attrib.GetValue()
            elif attrib.name == 'units':
                gpbVariable.units = attrib.GetValue()
            else:
                gpbVariable.other_attributes.add()
                gpbVariable.other_attributes[i].name = attrib.name
                gpbVariable.other_attributes[i].value = str(attrib.GetValue())
                i = i + 1

        #
        # Load up the dimensions of the variable, if there are any.
        #
        i = 0
        for dim in var.shape:
            gpbVariable.dimensions.add()
            gpbVariable.dimensions[i].name = dim.name
            gpbVariable.dimensions[i].length = dim.length
                
            i = i + 1


    def __loadGPBSourceMetaData(self, GPBSource, dSource, userProfile):
        log.debug("__loadGPSSourceMetaData()")
    
        #for attrib in var.attributes:
        for property in dSource.property:
            GPBSource.property.append(property)
            
        for station_id in dSource.station_id:
            GPBSource.station_id.append(station_id)
 
        GPBSource.request_type = dSource.request_type
        GPBSource.base_url = dSource.base_url
        GPBSource.max_ingest_millis = dSource.max_ingest_millis

        GPBSource.ion_title = dSource.ion_title
        GPBSource.ion_description = dSource.ion_description

        GPBSource.ion_name = userProfile.resource_reference.name
        GPBSource.ion_email = userProfile.resource_reference.email
        GPBSource.ion_institution = userProfile.resource_reference.institution

        GPBSource.visualization_url = dSource.visualization_url
        
        
    def __printSourceMetadata(self, dSource):
        log.debug('source_type: ' + str(dSource.source_type))
        for property in dSource.property:
            log.debug('Property: ' + property)
        for sid in dSource.station_id:
            log.debug('Station ID: ' + sid)
        log.debug('request_type: ' + str(dSource.request_type))
        log.debug('base_url: ' + dSource.base_url)
        log.debug('max_ingest_millis: ' + str(dSource.max_ingest_millis))

        
    def __loadGPBMinMetaData(self, rspGpb, dSet):
        i = 0
        rootAttributes = rspGpb.dataResourceSummary
        #
        # Load the "minimum metadata" portion of the dataset metadata into
        # keyword fields of the GPB, then load the "other attributes" of the
        # dataset into repeated name/value GPBs. 
        #
        for attrib in dSet.root_group.attributes:
            #log.debug('Root Attribute: %s = %s'  % (str(attrib.name), str(attrib.GetValue())))
            #log.debug('Root Attribute: %s = %s'  % (attrib.name, attrib.GetValue()))
            if attrib.name == 'title':
                rootAttributes.title = attrib.GetValue()
            elif attrib.name == 'institution':                
                rootAttributes.institution = attrib.GetValue()
            elif attrib.name == 'source':                
                rootAttributes.source = attrib.GetValue()
            elif attrib.name == 'references':                
                rootAttributes.references = attrib.GetValue()
            elif attrib.name == 'ion_time_coverage_start':                
                rootAttributes.ion_time_coverage_start = attrib.GetValue()
            elif attrib.name == 'ion_time_coverage_end':                
                rootAttributes.ion_time_coverage_end = attrib.GetValue()
            elif attrib.name == 'summary':                
                rootAttributes.summary = attrib.GetValue()
            elif attrib.name == 'comment':                
                rootAttributes.comment = attrib.GetValue()
            elif attrib.name == 'ion_geospatial_lat_min':                
                rootAttributes.ion_geospatial_lat_min = float(attrib.GetValue())
            elif attrib.name == 'ion_geospatial_lat_max':                
                rootAttributes.ion_geospatial_lat_max = float(attrib.GetValue())
            elif attrib.name == 'ion_geospatial_lon_min':                
                rootAttributes.ion_geospatial_lon_min = float(attrib.GetValue())
            elif attrib.name == 'ion_geospatial_lon_max':                
                rootAttributes.ion_geospatial_lon_max = float(attrib.GetValue())
            elif attrib.name == 'ion_geospatial_vertical_min':                
                rootAttributes.ion_geospatial_vertical_min = float(attrib.GetValue())
            elif attrib.name == 'ion_geospatial_vertical_max':                
                rootAttributes.ion_geospatial_vertical_max = float(attrib.GetValue())
            elif attrib.name == 'ion_geospatial_vertical_positive':                
                rootAttributes.ion_geospatial_vertical_positive = attrib.GetValue()
            else:
                rspGpb.other_attributes.add()
                rspGpb.other_attributes[i].name = attrib.name
                rspGpb.other_attributes[i].value = str(attrib.GetValue())
                i = i + 1
        
        #
        # Loup of the dimensions of the dataset
        #
        i = 0        
        for dim in dSet.root_group.dimensions:
            rspGpb.dimensions.add()
            rspGpb.dimensions[i].name = dim.name
            rspGpb.dimensions[i].length = dim.length
            i = i + 1



    @defer.inlineCallbacks
    def __getUserProfile(self, userID):
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
        OoiIdRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR get_user request')
        OoiIdRequest.configuration = IdentityRequest.CreateObject(USER_OOIID_TYPE)
        OoiIdRequest.configuration.ooi_id = userID
        try:
            result = yield self.irc.get_user(OoiIdRequest)
        except ReceivedApplicationError:
            defer.returnValue(None)
            
        defer.returnValue(result)            




