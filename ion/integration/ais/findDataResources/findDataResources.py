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
        self.dsID = None

    def setTestDatasetID(self, dsID):
        self.dsID = dsID
        
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
        # This is the way it will work normally:
        # resID = self.dscc.find_dataset_resources(msg)
        # but for until the dataset controller is ready, do this:
        resID = self.ais.getTestDatasetID()
        log.debug('DHE: Stub find_data_resources returned identity: ' + str(resID))
        
        log.debug('DHE: findDataResources getting resource instance')
        ds = yield self.rc.get_instance(resID)
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
        self.__loadDataResources(rspMsg.message_parameters_reference[0].dataResourceSummary[0], ds, userID, resID)

        defer.returnValue(rspMsg)

    def __loadDataResources(self, resSummary, ds, userID, resID):
        resSummary.user_ooi_id = userID
        resSummary.data_resource_id = resID
        resSummary.title = ds.root_group.FindAttributeByName('title').GetValue()
        resSummary.institution = ds.root_group.FindAttributeByName('institution').GetValue()
        resSummary.source = ds.root_group.FindAttributeByName('source').GetValue()
        resSummary.references = ds.root_group.FindAttributeByName('references').GetValue()
        resSummary.ion_time_coverage_start = ds.root_group.FindAttributeByName('ion_time_coverage_start').GetValue()
        resSummary.ion_time_coverage_end = ds.root_group.FindAttributeByName('ion_time_coverage_end').GetValue()
        #resSummary.summary = ds.root_group.FindAttributeByName('summary').GetValue()
        #resSummary.comment = ds.root_group.FindAttributeByName('comment').GetValue()
        resSummary.ion_geospatial_lat_min = float(ds.root_group.FindAttributeByName('ion_geospatial_lat_min').GetValue())
        resSummary.ion_geospatial_lat_max = float(ds.root_group.FindAttributeByName('ion_geospatial_lat_max').GetValue())
        resSummary.ion_geospatial_lon_min = float(ds.root_group.FindAttributeByName('ion_geospatial_lon_min').GetValue())
        resSummary.ion_geospatial_lon_max = float(ds.root_group.FindAttributeByName('ion_geospatial_lon_max').GetValue())
        resSummary.ion_geospatial_vertical_min = float(ds.root_group.FindAttributeByName('ion_geospatial_vertical_min').GetValue())
        resSummary.ion_geospatial_vertical_max = float(ds.root_group.FindAttributeByName('ion_geospatial_vertical_max').GetValue())
        resSummary.ion_geospatial_vertical_positive = ds.root_group.FindAttributeByName('ion_geospatial_vertical_positive').GetValue()
        lat = ds.root_group.FindVariableByName('lat')
        resSummary.standard_name  = lat.GetStandardName()
        resSummary.units = lat.GetUnits()
        

"""
   optional string long_name = 19;

"""
