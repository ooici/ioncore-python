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

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
#from ion.services.dm.inventory.dataset_controller import DatasetControllerClient
# DHE Temporarily pulling DatasetControllerClient from scaffolding
from ion.integration.ais.findDataResources.resourceStubs import DatasetControllerClient

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
        self.dscc = DatasetControllerClient()

        
    @defer.inlineCallbacks
    def findDataResources(self, msg):
        log.debug('findDataResources Worker Class got GPB: \n' + str(msg))

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
        
        resID = self.dscc.find_dataset_resources(msg)
        log.debug('DHE: Stub find_data_resources returned identity: ' + str(resID))
        
        log.debug('DHE: findDataResources getting resource instance')
        ds = yield self.rc.get_instance(resID)
        log.debug('DHE: get_instance returned ' + str(ds))

        """
        """

        for atrib in ds.root_group.attributes:
            print 'Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue()))

        for var in ds.root_group.variables:
            #print 'Root Variable: %s' % str(var.GetStandardName())
            print 'Root Variable: %s' % str(var.name)
            for atrib in var.attributes:
                print "Attribute: %s = %s" % (str(atrib.name), str(atrib.GetValue()))
            print "....Dimensions:"
            for dim in var.shape:
                print "    ....%s (%s)" % (str(dim.name), str(dim.length))


        lat = ds.root_group.FindVariableByName('lat')
        unicode_name  = lat.GetStandardName()
        unicode_units = lat.GetUnits()
        
        #dimensions    =  [str(dim.name) for dim in lat.shape]
        print 'YO! %s units are %s' % (str(unicode_name), str(unicode_units))
        #print 'YO YO! lat has %s contents' % (len(lat.content))
        #testvalue = lat.GetValue()
        #print 'YO YO YO! lat has value %s' % (str(testvalue))


        """
        lat = ds.root_group.FindAttributeByName('latitude')
        log.debug("Here are the latitude values")
        for value in lat.GetValues():
            log.debug(str(value))
        """

        rspMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        rspMsg.message_parameters_reference.add()
        rspMsg.message_parameters_reference[0] = rspMsg.CreateObject(FIND_DATA_RESOURCES_RSP_MSG_TYPE)
        rspMsg.message_parameters_reference[0].data_resource_id.append(resID)

        defer.returnValue(rspMsg)

