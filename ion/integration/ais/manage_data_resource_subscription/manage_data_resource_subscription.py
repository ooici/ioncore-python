#!/usr/bin/env python

"""
@file ion/integration/ais/manage_data_resource_subscription/manage_data_resource_subscription.py
@author Ian Katz
@brief The worker class that implements the subscribeDataResource function for the AIS  (workflow #105)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

from ion.services.coi.resource_registry.association_client import AssociationClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, \
                                                                    TYPE_OF_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID

from ion.services.coi.resource_registry.resource_client import ResourceClient, \
                                                                    ResourceInstance
from ion.services.coi.resource_registry.resource_client import ResourceClientError, \
                                                                    ResourceInstanceError

from ion.services.dm.distribution.publisher_subscriber import PublisherFactory
from ion.services.dm.distribution.events import NewSubscriptionEventPublisher, DelSubscriptionEventPublisher
from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient                                                         

from ion.core.intercept.policy import get_dispatcher_id_for_user

from ion.core.object import object_utils

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       SUBSCRIPTION_INFO_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_RSP_TYPE

#fixme, don't need all of these

DISPATCHER_RESOURCE_TYPE           = object_utils.create_type_identifier(object_id=7002, version=1)
DISPATCHER_WORKFLOW_RESOURCE_TYPE  = object_utils.create_type_identifier(object_id=7003, version=1)

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


class ManageDataResourceSubscription(object):

    def __init__(self, ais):
        log.debug('ManageDataResourceSubscription.__init__()')
        self.mc  = ais.mc
        self.rc  = ais.rc
        self.ac  = AssociationClient(proc=ais)
        self.pfn = PublisherFactory(publisher_type=NewSubscriptionEventPublisher, process=ais)
        self.pfd = PublisherFactory(publisher_type=DelSubscriptionEventPublisher, process=ais)
        self.nac = NotificationAlertServiceClient(proc=ais)


    def update(self, msg):
        """
        @brief update the subscription to a data resource 
        @param msg GPB, 9201/1, 
        @GPB{Input,9201,1}
        @GPB{Returns,9201,1}
        @retval success
        """
        log.info('ManageDataResourceSubscription.updateDataResourceSubscription()\n')

        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != SUBSCRIBE_DATA_RESOURCE_REQ_TYPE:
                errtext = "ManageDataResourceSubscription.createDataResourceSubscription(): " + \
                    "Expected SubscriptionCreateReqMsg type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            #FIXME: just delete and re-add

        except ReceivedApplicationError, ex:
            log.info('ManageDataResourceSubscription.createDataResourceSubscription(): Error attempting to FIXME: %s' %ex)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)

        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        #Response.message_parameters_reference.add()
        #Response.message_parameters_reference[0] = Response.CreateObject(CREATE_DATA_RESOURCE_RSP_TYPE)
        #Response.message_parameters_reference[0].data_source_id  = my_datasrc_id
        #Response.message_parameters_reference[0].data_set_id     = my_dataset_id
        #Response.message_parameters_reference[0].association_id  = association.AssociationIdentity
        defer.returnValue(Response)



        defer.returnValue(None)


    def delete(self, msg):
        """
        @brief delete the subscription to a data resource 
        @param msg GPB, 9211/1, 
        @GPB{Input,9201,1}
        @GPB{Returns,9201,1}
        @retval success
        """
        log.info('ManageDataResourceSubscription.delete()\n')

        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != SUBSCRIBE_DATA_RESOURCE_REQ_TYPE:
                errtext = "ManageDataResourceSubscription.createDataResourceSubscription(): " + \
                    "Expected SubscriptionCreateReqMsg type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            #check that we have GPB for subscription_modify_type
            #get msg. dispatcher_id, script_path, data_source_resource_id
            #check that dispatcher_id exists -- look up the resource gpb #7002

            # uncomment when ready
            #yield self._dispatcherUnSubscribe(user_ooi_id, data_set_id, dispatcher_script_path)

            #fixme: interact with mauice's code

        except ReceivedApplicationError, ex:
            log.info('ManageDataResourceSubscription.createDataResourceSubscription(): Error attempting to FIXME: %s' %ex)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)

        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        #Response.message_parameters_reference.add()
        #Response.message_parameters_reference[0] = Response.CreateObject(CREATE_DATA_RESOURCE_RSP_TYPE)
        #Response.message_parameters_reference[0].data_source_id  = my_datasrc_id
        #Response.message_parameters_reference[0].data_set_id     = my_dataset_id
        #Response.message_parameters_reference[0].association_id  = association.AssociationIdentity
        defer.returnValue(Response)

        
    @defer.inlineCallbacks
    def create(self, msg):
        """
        @brief subscribe to a data resource 
        @param msg GPB, 
        @GPB{Input,9203,1}
        @GPB{Returns,9204,1}
        @retval success
        """
        log.info('ManageDataResourceSubscription.createDataResourceSubscription()\n')
        log.debug('user_ooi_id = ' + msg.message_parameters_reference.subscriptionInfo.user_ooi_id)


        #### TEMPTEMPTEMP ####

        """
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(SUBSCRIBE_DATA_RESOURCE_RSP_TYPE)
        Response.message_parameters_reference[0].success  = True
        defer.returnValue(Response)
        """
        #### END TEMPTEMPTEMP ####

        try:
            
            # look at Maurice's service: notification alert service (test_notification_alert)
            
            ### NEW CODE START
            reqMsg = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='NAS Add Subscription request')
            reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIPTION_INFO_TYPE)
            reqMsg.message_parameters_reference.user_ooi_id = '0'
            reqMsg.message_parameters_reference.data_src_id = 'dataset123'
            reqMsg.message_parameters_reference.subscription_type = reqMsg.message_parameters_reference.SubscriptionType.EMAILANDDISPATCHER
            reqMsg.message_parameters_reference.email_alerts_filter = reqMsg.message_parameters_reference.AlertsFilter.UPDATES
            log.debug("createDataResourceSubscription calling notification alert service")
            #reply = yield self.nac.addSubscription(reqMsg)

            Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
            Response.message_parameters_reference.add()
            Response.message_parameters_reference[0] = Response.CreateObject(SUBSCRIBE_DATA_RESOURCE_RSP_TYPE)
        

            if False:
            #if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
                log.error('response is not an AIS_RESPONSE_MSG_TYPE GPB')
                Response.message_parameters_reference[0].success  = False
            else:            
                Response.message_parameters_reference[0].success  = True
            
            defer.returnValue(Response)


            ### NEW CODE END
            
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if msg.MessageType != SUBSCRIBE_DATA_RESOURCE_REQ_TYPE:
                errtext = "ManageDataResourceSubscription.createDataResourceSubscription(): " + \
                    "Expected SubscriptionCreateReqMsg type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if not (msg.IsFieldSet("user_ooi_id") and 
                    msg.IsFieldSet("data_source_id") and
                    msg.IsFieldSet("subscription_type")):

                errtext = "ManageDataResourceSubscription.createDataResourceSubscription(): " + \
                    "required fields not provided (user_ooi_id, data_ource_id, subscription_type)"
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  msg.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)


            if msg.DISPATCHER == msg.subscription_type or msg.EMAILANDDISPATCHER == msg.subscription_type:
                yield self._dispatcherSubscribe(user_ooi_id, data_source_id, msg.dispatcher_script_path)
                
            #FIXME: call maurice's code


        except ReceivedApplicationError, ex:
            log.info('ManageDataResourceSubscription.createDataResourceSubscription(): Error attempting to FIXME: %s' %ex)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)



        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)

        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.SubscribeObject(SUBSCRIBE_DATA_RESOURCE_RSP_TYPE)
        #FIXME
        defer.returnValue(Response)


    @defer.inlineCallbacks
    def _dispatcherSubscribe(self, user_ooi_id, data_source_id, dispatcher_script_path):
 
        dispatcher_id = get_dispatcher_queue_for_user(user_ooi_id)

        #get datasource/set association and resources
        tmp = yield self._getExistingResources(data_source_id, dispatcher_id)
        (dispatcher_resource, datasource_resource, dataset_resource) = tmp

        #associate: dispatcherresource has a dispatcherworkflowresource
        association = yield self.ac.create_association(dispatcher_resource, HAS_A_ID, workflow_resource)


        #publish event: new subscription, origin = dispatcher_id: content = dispatcherworkflowresource_id
        publisher = yield self.pfn.build(origin=dispatcher_id)

        #Create the dispatcher workflow resource
        dwr = yield self.rc.create_instance(DISPATCHER_WORKFLOW_RESOURCE_TYPE)

        dwr.dataset_id = dataset_resource.key
        dwr.workflow_path = dispatcher_script_path
        dwr.ResourcesLifecycleState = dwr.ACTIVE
        yield self.rc.put_instance(dwr)
        
                
        # Associate the workflow with the dispatcher_resource

        # Make sure the association doesn't already exist..
        assoc = yield self.ac.find_associations(self.dispatcher_resource, HAS_A_ID, dwr)
        if assoc is not None and len(assoc) > 0:
           log.warn("Association already exists! -- The dispatcher should already own this subscription!")
           defer.returnValue(None) 
        
        # Otherwise create the association
        assoc = yield self.ac.create_association(self.dispatcher_resource, HAS_A_ID, dwr)

        
        # Publish the new subscription notification
        yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)


        defer.returnValue(None)


    @defer.inlineCallbacks
    def _dispatcherUnSubscribe(self, user_ooi_id, data_source_id, dispatcher_script_path):
 
        dispatcher_id = get_dispatcher_queue_for_user(user_ooi_id)

        #get datasource/set association and resources
        tmp = yield self._getExistingResources(data_source_id, dispatcher_id)
        (dispatcher_resource, datasource_resource, dataset_resource) = tmp


        #publish event: delete subscription, origin = dispatcher_id: content = dispatcherworkflowresource_id
        publisher = yield pfd.build(origin=dispatcher_id)

        #Create the dispatcher workflow resource
        dwr = yield self.rc.create_instance(DISPATCHER_WORKFLOW_RESOURCE_TYPE)

        dwr.dataset_id = dataset_resource.key
        dwr.workflow_path = dispatcher_script_path
        
                
        # Publish the delete subscription notification
        yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)

        defer.returnValue(None)



    @defer.inlineCallbacks
    def _getExistingResources(self, ds_resource_id, dispatcher_id):
        """
        @brief get the resources we need: dispatcher, datasource, dataset
        @param ds_resource_id string
        @param dispatcher_id string
        @retval (dispatcher, datasource, dataset)
        """

        #FIXME: detect any errors here and bail (if any resources don't exist...)
        #fixme: resource visibility / permission check?
    
        dispatcher_resource = yield self.rc.get_instance(dispatcher_id)
        
        datasource_resource = yield self.rc.get_instance(ds_resource_id)
        
        dataset_resource    = yield self._getOneAssociationObject(datasource_resource, HAS_A_ID)
        
        defer.returnValue(dispatcher_resource, datasource_resource, dataset_resource)


    @defer.inlineCallbacks
    def _getOneAssociationObject(self, the_subject, the_predicate):
        """
        @brief get the subject side of an association when you only expect one
        @return id of what you're after
        """

        #can also do obj=
        found = yield self.ac.find_associations(subject=datasource_resource, \
                                                predicate_or_predicates=HAS_A_ID)

        association = None
        for a in found:
            exists = yield self.ac.association_exists(a.ObjectReference.key, TYPE_OF_ID, DATASOURCE_RESOURCE_TYPE_ID)
            if exists:
                #FIXME: if not association is None then we have data inconsistency!
                association = a

        #FIXME: if association is None: ERRORZ

        the_resource = yield self.rc.get_associated_resource_object(association)
        defer.returnValue(the_resource)




    @defer.inlineCallbacks
    def _getOneAssociationSubject(self, the_predicate, the_object):
        """
        @brief get the subject side of an association when you only expect one
        @return id of what you're after
        """

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


        #alternate method: (david says DON'T)
        #dataset_reference = association.subject_reference
        #dataset_resource = yield self.rc.get_instance(dataset_reference)

        the_resource = yield self.rc.get_associated_resource_subject(association)
        defer.returnValue(the_resource)
