#!/usr/bin/env python

"""
@file ion/integration/ais/createDownloadURL/createDownloadURL.py
@author David Everett
@brief Worker class to construct the download URL for given data resource
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
#from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
# DHE Temporarily pulling DatasetControllerClient from scaffolding
from ion.integration.ais.findDataResources.resourceStubs import DatasetControllerClient

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE

from ion.integration.ais.ais_object_identifiers import CREATE_DOWNLOAD_URL_RSP_MSG_TYPE

DNLD_BASE_THREDDS_URL = 'http://localhost:8081/thredds'
DNLD_DIR_PATH = '/dodsC/scanData/'
DNLD_FILE_TYPE = '.ncml'
    

class CreateDownloadURL(object):

    def __init__(self, ais):
        log.info('CreateDownloadURL.__init__()')
        self.rc = ResourceClient()
        self.mc = ais.mc
        self.dscc = DatasetControllerClient()

    @defer.inlineCallbacks
    def createDownloadURL(self, msg):
        log.debug('createDownloadURL Worker Class got GPB: \n' + str(msg))


        if msg.message_parameters_reference.IsFieldSet('data_resource_id'):
            resID = msg.message_parameters_reference.data_resource_id
        else:
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                  MessageName='AIS createDownloadURL error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "Required field [data_resource_id] not found in message"
            defer.returnValue(Response)
            
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(CREATE_DOWNLOAD_URL_RSP_MSG_TYPE)
        #
        #  opendap URL for accessing the data.
        # The URL will be composed a couple parts:  <base_url_to_thredds> +
        # <directory_path> + <resourceid>.ncml
        #
        # http://localhost:8081/thredds/dodsC/scanData/<resID>.ncml
        #
        rspMsg.message_parameters_reference[0].download_url = \
            DNLD_BASE_THREDDS_URL + \
            DNLD_DIR_PATH + \
            resID + \
            DNLD_FILE_TYPE
                            
        defer.returnValue(rspMsg)


