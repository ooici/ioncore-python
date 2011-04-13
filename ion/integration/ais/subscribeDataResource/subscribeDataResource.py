#!/usr/bin/env python

"""
@file ion/integration/ais/subscribeDataResource/subscribeDataResource.py
@author Ian Katz
@brief The worker class that implements the subscribeDataResource function for the AIS  (workflow #105)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

from ion.services.coi.resource_registry_beta.association_client import AssociationClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, \
                                                                    TYPE_OF_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID

from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, \
                                                                    ResourceInstance
from ion.services.coi.resource_registry_beta.resource_client import ResourceClientError, \
                                                                    ResourceInstanceError

from ion.core.object import object_utils

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       OOI_ID_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_RSP_TYPE

#fixme, don't need all of these

RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
message ResourceConfigurationRequest{
    enum _MessageTypeIdentifier {
      _ID = 10;
      _VERSION = 1;
    }

    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
"""

RESOURCE_CFG_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
message ResourceConfigurationResponse{
    enum _MessageTypeIdentifier {
      _ID = 12;
      _VERSION = 1;
    }

    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;

    optional string result = 3;
}
"""




class SubscribeDataResource(object):

    def __init__(self, ais):
        log.debug('SubscribeDataResource.__init__()')
        self.mc    = ais.mc
        self.rc    = ais.rc
        self.ac    = AssociationClient(proc=ais)

    def deleteDataResourceSubscription(self, msg):
        """
        @brief delete the subscription to a data resource 
        @param msg GPB, 9211/1, 
        @GPB{Input,9207,1}
        @GPB{Returns,9208,1}
        @retval success
        """
        log.info('SubscribeDataResource.modifyDataResourceSubscription()\n')
        #check that we have GPB for subscription_modify_type
        #get msg. dispatcher_id, script_path, data_source_resource_id
        #check that dispatcher_id exists -- look up the resource gpb #7002
        #get dispatcher id, name queue
        #check that datasource_resource exists, look it up
        #fixme: resource visibility / permission check?
        #get datasource/set association
        #check that dataset_id exists, look it up

        #look up 
        #create new dispatcherworkflowresource #7003
        #associate: dispatcherresource has a dispatcherworkflowresource
        #publish event: delete subscription, origin = dispatcher_id: content = dispatcherworkflowresource_id

        

    def createDataResourceSubscription(self, msg):
        """
        @brief subscribe to a data resource 
        @param msg GPB, 
        @GPB{Input,9203,1}
        @GPB{Returns,9204,1}
        @retval success
        """
        log.info('SubscribeDataResource.createDataResourceSubscription()\n')
        


        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != SUBSCRIBE_DATA_RESOURCE_REQ_TYPE:
                errtext = "SubscribeDataResource.createDataResourceSubscription(): " + \
                    "Expected SubscriptionCreateReqMsg type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                                         MessageName='AIS SubscribeDataResource error response')
                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if not (msg.IsFieldSet("dispatcher_id") and 
                    msg.IsFieldSet("script_path") and 
                    msg.IsFieldSet("resource_id")):

                errtext = "SubscribeDataResource.createDataResourceSubscription(): " + \
                    "required fields not provided (dispatcher_id, script_path, resource_id)"
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                                         MessageName='AIS SubscribeDataResource error response')
                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)


            #msg.dispatcher_id
            #msg.script_path
            #msg.resource_id



            
            (f,ix,me) = self._getExistingResources(msg.resource_id, msg.dispatcher_id)

        #get datasource/set association
        #check that dataset_id exists, look it up

        #create new dispatcherworkflowresource #7003
        #associate: dispatcherresource has a dispatcherworkflowresource
        #publish event: new subscription, origin = dispatcher_id: content = dispatcherworkflowresource_id





            #FIXME, associate them!
            #association_id = somepartof self.rc.create_association(subject, predicate, object)

            marked = yield self._markResourceLifecycles([datasrc_resource, dataset_resource, association_resource],
                                                        datasrc_resource.ACTIVE, datasrc_resource.RETIRED)
            if not marked:
                pass #fixme, raise error to do the cleanup
                


        except ReceivedApplicationError, ex:
            log.info('SubscribeDataResource.createDataResourceSubscription(): Error attempting to FIXME: %s' %ex)

            yield self._markResourceLifecycles([datasrc_resource, dataset_resource, association_resource], 
                                               datasrc_resource.RETIRED, datasrc_resource.RETIRED)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE,
                                                     MessageName='AIS SubscribeDataResource error response')
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)



        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE,
                                                 MessageName='AIS SubscribeDataResource response')
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.SubscribeObject(SUBSCRIBE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].data_source_id  = my_datasrc_id
        Response.message_parameters_reference[0].data_set_id     = my_dataset_id
        Response.message_parameters_reference[0].association_id  = FIXME
        defer.returnValue(Response)




    @defer.inlineCallbacks
    def _getExistingResources(self, resource_id, dispatcher_id):
        """
        @brief get the resources we need: dispatcher, datasource, dataset
        @param resource_id string
        @param dispatcher_id string
        @retval (dispatcher, datasource, dataset)
        """

        #FIXME: detect any errors here and bail (if any resources don't exist...)
        #fixme: resource visibility / permission check?
    
        dispatcher_resource = yield self.rc.get_instance(dispatcher_id)
        
        datasource_resource = yield self.rc.get_instance(resource_id)
        
        #association crap
        #can also do subject= 
        found = yield self.ac.find_associations(obj=datasource_resource, \
                                                    predicate_or_predicates=HAS_A_ID)

        association = None
        for a in found:
            #ian is not convinced... but this should work
            exists = yield self.ac.association_exists(a.SubjectReference.key, TYPE_OF_ID, DATASET_RESOURCE_TYPE_ID)
            if exists:
                #FIXME: if not association is None then we have data inconsistency!
                association = a

        #FIXME: if association is None: ERRORZ

        dataset_resource = yield self.rc.get_associated_resource_subject(association)

        #alternate method: (david says DON'T)
        #dataset_reference = association.subject_reference
        #dataset_resource = yield self.rc.get_instance(dataset_reference)
        
        defer.returnValue(dispatcher_resource, datasource_resource, dataset_resource)
