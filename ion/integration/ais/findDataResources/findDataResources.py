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

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
#from ion.integration.ais.findDataResources.resourceStubs import ResourceClient, ResourceInstance

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import FIND_DATA_RESOURCES_REQ_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import FIND_DATA_RESOURCES_RSP_MSG_TYPE

from ion.core.object import object_utils

class FindDataResources(object):
    
    def __init__(self, ais):
        log.info('FindDataResources.__init__()')
        self.rc = ResourceClient()
        self.mc = ais.mc

        
    @defer.inlineCallbacks
    def findDataResources(self, msg):
        log.debug("findDataResources Worker Class!")

        """
        Need to build up a GPB Message;
         - get request message object from message client
         - build up request message to resource client based on incoming
           request message
         - send to worker resource client to get list of resource ids
         - get results
         - for each resource id, determine if in bounds
         - get response message object from messagse client
         - build up response message
         - get response message payload
         - build up response message payload
         - return response message to ais service
        """
        
        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_RSP_MSG_TYPE)
        
        defer.returnValue('something useful')

        # DHE TEST!!!
        #log.debug("build objects!")
        #yield self.rc.build_objects()

