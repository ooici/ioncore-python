#!/usr/bin/env python

"""
@file ion/integration/ais/getDataResourceDetail/getDataResourceDetail.py
@author David Everett
@brief Worker class to get the resource metadata for a given data resource
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
#from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
# DHE Temporarily pulling DatasetControllerClient from scaffolding
from ion.integration.ais.findDataResources.resourceStubs import DatasetControllerClient
#from ion.integration.ais.getDataResourceDetail.cfdata import cfData

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE

from ion.integration.ais.ais_object_identifiers import GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE

class GetDataResourceDetail(object):
    
    def __init__(self, ais):
        log.info('GetDataResourceDetail.__init__()')
        self.ais = ais
        self.rc = ResourceClient()
        self.mc = ais.mc
        self.dscc = DatasetControllerClient()

        
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
            
        log.debug('DHE: getDataResourceDetail will get dataset instance: ' + str(resID))

        try:        
            log.debug('DHE: getDataResourceDetail getting resource instance')
            ds = yield self.rc.get_instance(resID)

            for atrib in ds.root_group.attributes:
                log.debug('Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue())))
    
            for var in ds.root_group.variables:
                log.debug('Root Variable: %s' % str(var.name))
                for atrib in var.attributes:
                    log.debug("Attribute: %s = %s" % (str(atrib.name), str(atrib.GetValue())))
                log.debug("....Dimensions:")
                for dim in var.shape:
                    log.debug("    ....%s (%s)" % (str(dim.name), str(dim.length)))

        except ReceivedApplicationError, ex:
            # build AIS error response
            RspMsg = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                MessageName='AIS getDataResourceDetail error response')
            RspMsg.error_num = ex.msg_content.MessageResponseCode
            RspMsg.error_str = ex.msg_content.MessageResponseBody
            defer.returnValue(RspMsg)

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE)

        rspMsg.message_parameters_reference[0].data_resource_id = resID
        # Fill in the rest of the message with the CF metadata

        i = 0
        for var in ds.root_group.variables:
            print 'Working on variable: %s' % str(var.name)
            rspMsg.message_parameters_reference[0].variable.add()
            self.__loadRootVariable(rspMsg.message_parameters_reference[0].variable[i], ds, var)
            i = i + 1
        
        defer.returnValue(rspMsg)


    def __loadRootVariable(self, rootVariable, ds, var):
        for atrib in var.attributes:
            tmpstr = str(atrib.name) + '::' + str(atrib.GetValue())
            rootVariable.other_attributes.append(tmpstr)

        """
        try:
            rootVariable.standard_name  = var.GetStandardName()
            rootVariable.units = var.GetUnits()
            
        except:            
            estr = 'Object ERROR!'
            log.exception(estr)
         """

    def __loadSourceMetaData(self, source, ds, var):
        log.debug(__loadSourceMetaData)
    
        """
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


