#!/usr/bin/env python

"""
@file ion/integration/ais/getDataResourceDetail/getDataResourceDetail.py
@author David Everett
@brief Worker class to get the resource metadata for a given data resource
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.exception import ReceivedApplicationError
from ion.services.coi.resource_registry.resource_client import ResourceClient
#from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
#from ion.integration.ais.getDataResourceDetail.cfdata import cfData

# Temporary, until assocation of dataset and datasource are there.
from ion.integration.ais.findDataResources.findDataResources import FindDataResources

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE

from ion.integration.ais.ais_object_identifiers import GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import DATASOURCE_RESOURCE_TYPE_ID

class GetDataResourceDetail(object):
    
    def __init__(self, ais):
        log.info('GetDataResourceDetail.__init__()')
        self.ais = ais
        self.rc = ResourceClient()
        self.mc = ais.mc

        
    @defer.inlineCallbacks
    def getDataResourceDetail(self, msg):
        log.debug('getDataResourceDetail Worker Class got GPB: \n' + str(msg))

        if msg.message_parameters_reference.IsFieldSet('data_resource_id'):
            resID = msg.message_parameters_reference.data_resource_id
        else:
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS getDataResourceDetail error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "Required field [data_resource_id] not found in message"
            defer.returnValue(Response)
            
        try:        
            log.debug('getDataResourceDetail getting dataset resource instance: ' + resID)
            ds = yield self.rc.get_instance(resID)

            """
            for atrib in ds.root_group.attributes:
                log.debug('Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue())))
    
            for var in ds.root_group.variables:
                log.debug('Root Variable: %s' % str(var.name))
                for atrib in var.attributes:
                    log.debug("Attribute: %s = %s" % (str(atrib.name), str(atrib.GetValue())))
                log.debug("....Dimensions:")
                for dim in var.shape:
                    log.debug("    ....%s (%s)" % (str(dim.name), str(dim.length)))
            """                                    

        except ReceivedApplicationError, ex:
            # build AIS error response
            RspMsg = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                MessageName='AIS getDataResourceDetail error response')
            RspMsg.error_num = ex.msg_content.MessageResponseCode
            RspMsg.error_str = ex.msg_content.MessageResponseBody
            defer.returnValue(RspMsg)

        #
        # Find the datasource associated with this dataset; for now, instantiate
        # a FindDataResources worker object. The getAssociatedSource should be
        # moved into a common worker class; it's currently in the FindDataResources
        # class, which doesn't use it.
        #
        log.debug('getDataResourceDetail getting datasource resource instance')
        worker = FindDataResources(self.ais)
        dSourceResID = None
        dSourceResID = yield worker.getAssociatedSource(resID)

        if not (dSourceResID is None):
            log.debug('Associated datasourceID: ' + dSourceResID)
            
            dSource = yield self.rc.get_instance(dSourceResID)
        else:            
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS getDataResourceDetail error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = "No Associated Data Source Found"
            defer.returnValue(Response)

        self.__printSourceMetadata(dSource)
        
        
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE)

        rspMsg.message_parameters_reference[0].data_resource_id = resID

        self.__loadGPBMinMetaData(rspMsg.message_parameters_reference[0].dataResourceSummary, ds)
        self.__loadGPBSourceMetaData(rspMsg.message_parameters_reference[0].source, dSource)
        
        i = 0
        for var in ds.root_group.variables:
            print 'Working on variable: %s' % str(var.name)
            rspMsg.message_parameters_reference[0].variable.add()
            self.__loadGPBVariable(rspMsg.message_parameters_reference[0].variable[i], ds, var)
            i = i + 1

        defer.returnValue(rspMsg)


    def __loadGPBVariable(self, gpbVariable, ds, var):
        i = 0
        for attrib in var.attributes:
            if attrib.name == 'standard_name':
                gpbVariable.standard_name = attrib.GetValue()
            elif attrib.name == 'long_name':
                gpbVariable.long_name = attrib.GetValue()
            elif attrib.name == 'units':
                gpbVariable.units = attrib.GetValue()
            else:
                if i > 0:
                    tmpstr = '::'
                else:
                    tmpstr = ''
                tmpstr = tmpstr + str(attrib.name) + '=' + str(attrib.GetValue())
                gpbVariable.other_attributes.append(tmpstr)
                i = i + 1


    def __loadGPBSourceMetaData(self, GPBSource, dSource):
        log.debug("__loadGPSSourceMetaData()")
    
        #for attrib in var.attributes:
        for property in dSource.property:
            GPBSource.property.append(property)
            
        for station_id in dSource.station_id:
            GPBSource.station_id.append(station_id)
 
        GPBSource.request_type = dSource.request_type
        GPBSource.base_url = dSource.base_url
        GPBSource.max_ingest_millis = dSource.max_ingest_millis

        """
        This is the actual GPB
        optional net.ooici.services.sa.SourceType source_type = 1;
        repeated string property   = 2;
        repeated string station_id = 3;
    
        optional net.ooici.services.sa.RequestType request_type = 4;
        optional double top    = 5;
        optional double bottom = 6;
        optional double left   = 7;
        optional double right  = 8;
        optional string base_url    = 9;
        optional string dataset_url = 10;
        optional string ncml_mask   = 11;
        optional uint64 max_ingest_millis = 12;
    
        optional string start_time = 13;
        optional string end_time   = 14;
        optional string institution_id = 15;
        """
        
    def __printSourceMetadata(self, dSource):
        log.debug('source_type: ' + str(dSource.source_type))
        for property in dSource.property:
            log.debug('Property: ' + property)
        for sid in dSource.station_id:
            log.debug('Station ID: ' + sid)
        log.debug('request_type: ' + str(dSource.request_type))
        log.debug('base_url: ' + dSource.base_url)
        log.debug('max_ingest_millis: ' + str(dSource.max_ingest_millis))

        
    def __loadGPBMinMetaData(self, rootAttributes, dSet):
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




