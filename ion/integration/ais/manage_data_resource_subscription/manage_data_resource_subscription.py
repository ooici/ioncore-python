#!/usr/bin/env python

"""
@file ion/integration/ais/manage_data_resource_subscription/manage_data_resource_subscription.py
@author Dave Everett, Bill Bollenbacher
@brief The worker class that implements the subscribeDataResource function for the AIS  (workflow #105)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import logging
from twisted.internet import defer
import time

from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError, ApplicationError

from ion.services.coi.resource_registry.association_client import AssociationClient, AssociationClientError
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, \
                                                                    TYPE_OF_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID, \
                                                                    DISPATCHER_RESOURCE_TYPE_ID

from ion.services.coi.resource_registry.resource_client import ResourceClient, \
                                                                    ResourceInstance
from ion.services.coi.resource_registry.resource_client import ResourceClientError, \
                                                                    ResourceInstanceError

from ion.services.dm.distribution.publisher_subscriber import PublisherFactory
from ion.services.dm.distribution.events import NewSubscriptionEventPublisher, DelSubscriptionEventPublisher
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, SUBJECT_PREDICATE_QUERY_TYPE, IDREF_TYPE
from ion.util.iontime import IonTime

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient                                                         

from ion.integration.ais.common.spatial_temporal_bounds import SpatialTemporalBounds
from ion.integration.ais.common.metadata_cache import  MetadataCache

from ion.core.object import object_utils

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       SUBSCRIPTION_INFO_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_RSP_TYPE, \
                                                       GET_SUBSCRIPTION_LIST_REQ_TYPE, \
                                                       GET_SUBSCRIPTION_LIST_RESP_TYPE, \
                                                       FIND_DATA_SUBSCRIPTIONS_RSP_TYPE, \
                                                       DELETE_SUBSCRIPTION_REQ_TYPE, \
                                                       DELETE_SUBSCRIPTION_RSP_TYPE, \
                                                       UPDATE_SUBSCRIPTION_REQ_TYPE, \
                                                       UPDATE_SUBSCRIPTION_RSP_TYPE


#fixme, don't need all of these

PREDICATE_REFERENCE_TYPE           = object_utils.create_type_identifier(object_id=25, version=1)
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
        self.asc = AssociationServiceClient()

        self.ais = ais
        # Lazy initialize this when it is needed
        #self.pfn = PublisherFactory(publisher_type=NewSubscriptionEventPublisher, process=ais)
        self.pfn = None

        # Lazy initialize this when it is needed
        #self.pfd = PublisherFactory(publisher_type=DelSubscriptionEventPublisher, process=ais)
        self.pfd = None

        self.nac = NotificationAlertServiceClient(proc=ais)
        self.metadataCache = ais.getMetadataCache()
 

    @defer.inlineCallbacks
    def update(self, msg):
        """
        @brief update the subscription to a data resource 
        @param msg GPB,  
        @GPB{Input,9209,1}
        @GPB{Returns,9210,1}
        @retval success
        """
        log.info('ManageDataResourceSubscription.update()\n')
        # repackage the subscription info into a one item list for the delete() call
        reqMsg = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(DELETE_SUBSCRIPTION_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptions.add();
        reqMsg.message_parameters_reference.subscriptions[0].user_ooi_id  = msg.message_parameters_reference.subscriptionInfo.user_ooi_id
        reqMsg.message_parameters_reference.subscriptions[0].data_src_id  = msg.message_parameters_reference.subscriptionInfo.data_src_id
        Response = yield self.delete(reqMsg)
        if Response.MessageType != AIS_RESPONSE_ERROR_TYPE:
            Response = yield self.create(msg)
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
        if  self.ais.AnalyzeTiming != None:
            self.ais.TimeStamps.StartTime = time.time()
            self.ais.TimeStamps.LastTime = self.ais.TimeStamps.StartTime
            log.warning('ManageDataResourceSubscription.create: started at ' + str(self.ais.TimeStamps.StartTime))

        # check that the GPB is correct type & has a payload
        result = yield self._CheckRequest(msg)
        if result != None:
            result.error_str = "AIS.ManageDataResourceSubscription.create: " + result.error_str
            defer.returnValue(result)

        # check that subscriptionInfo is present in GPB
        if not msg.message_parameters_reference.IsFieldSet('subscriptionInfo'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "AIS.ManageDataResourceSubscription.create: Required field [subscriptionInfo] not found in message"
             defer.returnValue(Response)

        # check that AisDatasetMetadataType is present in GPB
        if not msg.message_parameters_reference.IsFieldSet('datasetMetadata'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "AIS.ManageDataResourceSubscription.create: Required field [datasetMetadata] not found in message"
             defer.returnValue(Response)

        # check that ooi_id is present in GPB
        if not msg.message_parameters_reference.subscriptionInfo.IsFieldSet('user_ooi_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "AIS.ManageDataResourceSubscription.create: Required field [user_ooi_id] not found in message"
             defer.returnValue(Response)

        if not msg.message_parameters_reference.subscriptionInfo.IsFieldSet('data_src_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "AIS.ManageDataResourceSubscription.create: Required field [data_src_id] not found in message"
             defer.returnValue(Response)

        # check that subscription type enum is present in GPB
        if not msg.message_parameters_reference.subscriptionInfo.IsFieldSet('subscription_type'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "AIS.ManageDataResourceSubscription.create: Required field [subscription_type] not found in message"
             defer.returnValue(Response)

        userID = msg.message_parameters_reference.subscriptionInfo.user_ooi_id
        msg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        try:
            log.debug("create: calling notification alert service addSubscription()")
            reply = yield self.nac.addSubscription(msg)
            if  self.ais.AnalyzeTiming != None:
                log.warning('ManageDataResourceSubscription.create: added subscription ' + self.ais.TimeStamp())
 
            Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
            Response.message_parameters_reference.add()
            
            #
            # Now determine the subscription type; if dispatcher, we need to create
            # a dispatcher workflow.  But first:
            # 1. Find the dispatcher associated with this user.
            # 2. Find the dispatcher workflow associated with this subscription.
            # 3. Delete the dispatcher workflow.
            #
            if ((msg.message_parameters_reference.subscriptionInfo.subscription_type == msg.message_parameters_reference.subscriptionInfo.SubscriptionType.DISPATCHER) or 
               (msg.message_parameters_reference.subscriptionInfo.subscription_type == msg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER)):

                #
                # There should be a dispatcher associated with this user; find it now.
                #

                log.info('Getting user resource instance')
                try:
                    self.userRes = yield self.rc.get_instance(userID)
                except ResourceClientError:
                    errString = 'Error getting instance of userID: ' + userID
                    log.error(errString)
                    # build AIS error response
                    Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                    Response.error_num = Response.ResponseCodes.INTERNAL_SERVER_ERROR
                    Response.error_str = "AIS.ManageDataResourceSubscription.create: " + errString
                    defer.returnValue(Response)
                log.info('Got user resource instance: ' + self.userRes.ResourceIdentity)
                self.userID = self.userRes.ResourceIdentity
                if  self.ais.AnalyzeTiming != None:
                    log.warning('ManageDataResourceSubscription.create: got user instance ' + self.ais.TimeStamp())

                dispatcherID = yield self.__findDispatcher(self.userRes)
                if (dispatcherID is None):
                    errString = 'Dispatcher not found for userID' + self.userID
                    log.error(errString)
                    # build AIS error response
                    Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                    Response.error_num = Response.ResponseCodes.NOT_FOUND
                    Response.error_str = "AIS.ManageDataResourceSubscription.create: " + errString
                    defer.returnValue(Response)
                else:
                    log.info('FOUND DISPATCHER: ' + dispatcherID)
                if  self.ais.AnalyzeTiming != None:
                    log.warning('ManageDataResourceSubscription.create: found dispatcher ' + self.ais.TimeStamp())

                #
                # Create a dispatcher workflow
                #
                yield self.__createDispatcherWorkflow(msg.message_parameters_reference, dispatcherID)
                if  self.ais.AnalyzeTiming != None:
                    log.warning('ManageDataResourceSubscription.create: created workflow ' + self.ais.TimeStamp())

            Response.message_parameters_reference[0] = Response.CreateObject(SUBSCRIBE_DATA_RESOURCE_RSP_TYPE)
            Response.message_parameters_reference[0].success  = True

            defer.returnValue(Response)

        except ReceivedApplicationError, ex:
            log.info('ManageDataResourceSubscription.createDataResourceSubscription(): Error attempting to addSubscription(): %s' %ex.msg_content.MessageResponseBody)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  "AIS.ManageDataResourceSubscription.create: " + ex.msg_content.MessageResponseBody
            defer.returnValue(Response)


    @defer.inlineCallbacks
    def __findDispatcher(self, userRes):

        # get the user's associations
        Associations = yield self.ac.find_associations(subject=userRes)       
        log.debug('Found ' + str(len(Associations)) + ' associations for user ' + userRes.ResourceIdentity)
        
        # get the dispatcher resources out of the Association Service
        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
        pair = request.pairs.add()
  
        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID
        pair.predicate = pref
  
        # Set the Object search term
        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = DISPATCHER_RESOURCE_TYPE_ID    
        pair.object = type_ref
  
        Dispatchers = yield self.asc.get_subjects(request)     
        log.debug('Found ' + str(len(Dispatchers.idrefs)) + ' dispatchers.')
        
        for Association in Associations:
            for Dispatcher in Dispatchers.idrefs:
                log.info('a=%s, d=%s'%(str(Association.ObjectReference.key), str(Dispatcher.key)))
                if Association.ObjectReference.key == Dispatcher.key:
                    defer.returnValue(Dispatcher.key)
        defer.returnValue(None)
        

    @defer.inlineCallbacks
    def __createDispatcherWorkflow(self, createInfo, dispatcherID):
 
        if  self.ais.AnalyzeTiming != None:
            log.warning('ManageDataResourceSubscription.__createDispatcherWorkflow: started at ' + self.ais.TimeStamp())

        log.debug('__createDispatcherWorkflow')

        dispatcherRes = yield self.rc.get_instance(dispatcherID)
        subscriptionInfo = createInfo.subscriptionInfo
        datasetInfo = createInfo.datasetMetadata
        if  self.ais.AnalyzeTiming != None:
            log.warning('ManageDataResourceSubscription.__createDispatcherWorkflow: got dispatcher instance ' + self.ais.TimeStamp())

        #
        # Create the dispatcher workflow resource
        #
        dwfRes = yield self.rc.create_instance(DISPATCHER_WORKFLOW_RESOURCE_TYPE, ResourceName = 'DispatcherWorkflow')
        workflowID = dwfRes.ResourceIdentity
        dwfRes.dataset_id = datasetInfo.data_resource_id
        dwfRes.user_ooi_id = subscriptionInfo.user_ooi_id
        dwfRes.workflow_path = subscriptionInfo.dispatcher_script_path
        yield self.rc.put_instance(dwfRes)
        if  self.ais.AnalyzeTiming != None:
            log.warning('ManageDataResourceSubscription.__createDispatcherWorkflow: put workflow instance ' + self.ais.TimeStamp())

        log.debug('Creating association between dispatcherID: ' + dispatcherID + ' and workflowID: ' + workflowID)        

        #
        # Create an association between the workflow and the dispatcher
        #
        try:
            association = yield self.ac.create_association(dispatcherRes, HAS_A_ID, dwfRes)
            if association not in dispatcherRes.ResourceAssociationsAsSubject:
                log.error('Error: subject not in association!')
            if association not in dwfRes.ResourceAssociationsAsObject:
                log.error('Error: object not in association')
            
            #
            # Put the association in datastore
            #
            log.debug('Storing association: ' + str(association))
            yield self.rc.put_instance(association)
            if  self.ais.AnalyzeTiming != None:
                log.warning('ManageDataResourceSubscription.__createDispatcherWorkflow: put association ' + self.ais.TimeStamp())

        except AssociationClientError, ex:
            errString = 'Error creating assocation between dispatcherID: ' + dispatcherID + ' and workflowID: ' + workflowID + '. ex: ' + str(ex)
            log.error(errString)
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num = Response.ResponseCodes.INTERNAL_SERVER_ERROR
            Response.error_str = errString
            defer.returnValue(Response)


        if self.pfn is None:
            pubfact = PublisherFactory(publisher_type=NewSubscriptionEventPublisher, process=self.ais)
            self.pfn = yield pubfact.build()
            if  self.ais.AnalyzeTiming != None:
                log.warning('ManageDataResourceSubscription.__createDispatcherWorkflow: built publisher factory ' + self.ais.TimeStamp())

                
        # Publish the new subscription notification
        yield self.pfn.create_and_publish_event(dispatcher_workflow = dwfRes.ResourceObject, origin = dispatcherID)
        if  self.ais.AnalyzeTiming != None:
            log.warning('ManageDataResourceSubscription.__createDispatcherWorkflow: published event ' + self.ais.TimeStamp())

        defer.returnValue(None)


    @defer.inlineCallbacks
    def delete(self, msg):
        """
        @brief delete the subscription to a data resource 
        @param msg GPB, 
        @GPB{Input,9205,1}
        @GPB{Returns,9206,1}
        @retval success
        """
        if  self.ais.AnalyzeTiming != None:
            self.ais.TimeStamps.StartTime = time.time()
            self.ais.TimeStamps.LastTime = self.ais.TimeStamps.StartTime
            log.warning('ManageDataResourceSubscription.delete: started at ' + str(self.ais.TimeStamps.StartTime))

        # check that the GPB is correct type & has a payload
        result = yield self._CheckRequest(msg)
        if result != None:
            result.error_str = "AIS.ManageDataResourceSubscription.delete: " + result.error_str
            defer.returnValue(result)

        # check that subscriptionInfo is present in GPB
        if not msg.message_parameters_reference.IsFieldSet('subscriptions'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "AIS.ManageDataResourceSubscription.delete: Required field [subscriptions] not found in message"
             defer.returnValue(Response)
             
        for Subscription in msg.message_parameters_reference.subscriptions:
            # check that user_ooi_id is present in GPB
            if not Subscription.IsFieldSet('user_ooi_id'):
                # build AIS error response
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
                Response.error_num = Response.ResponseCodes.BAD_REQUEST
                Response.error_str = "AIS.ManageDataResourceSubscription.delete: Required field [user_ooi_id] not found in message"
                defer.returnValue(Response)
    
            # check that data_src_id is present in GPB
            if not Subscription.IsFieldSet('data_src_id'):
                # build AIS error response
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
                Response.error_num = Response.ResponseCodes.BAD_REQUEST
                Response.error_str = "AIS.ManageDataResourceSubscription.delete: Required field [data_src_id] not found in message"
                defer.returnValue(Response)

            reqMsg = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
            reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
            reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = Subscription.user_ooi_id
            reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = Subscription.data_src_id

            try:
                log.debug("delete: calling notification alert service getSubscription()")
                subscription = yield self.nac.getSubscription(reqMsg)
                log.info('getSubscription returned:\n %s'%subscription.message_parameters_reference[0].subscriptionListResults[0])
                if  self.ais.AnalyzeTiming != None:
                    log.warning('ManageDataResourceSubscription.delete: got subscription ' + self.ais.TimeStamp())
    
                log.debug("delete: calling notification alert service removeSubscription()")
                reply = yield self.nac.removeSubscription(reqMsg)
                if  self.ais.AnalyzeTiming != None:
                    log.warning('ManageDataResourceSubscription.delete: removed subscription ' + self.ais.TimeStamp())
   
                # Now determine if subscription type includes a dispatcher.  If so, we need to delete
                # the dispatcher workflow by:
                #   1. Finding the dispatcher associated with this user.
                #   2. Finding the dispatcher workflow associated with this subscription.
                #   3. Deleting the dispatcher workflow.
    
                SubscriptionInfo = subscription.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo
                if ((SubscriptionInfo.subscription_type == SubscriptionInfo.SubscriptionType.DISPATCHER) or 
                    (SubscriptionInfo.subscription_type == SubscriptionInfo.SubscriptionType.EMAILANDDISPATCHER)):
                    log.info("delete: deleting dispatcher workflow")
    
                    log.info('Getting user resource instance')
                    UserID = reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id
                    try:
                        self.userRes = yield self.rc.get_instance(UserID)
                    except ResourceClientError:
                        errString = 'Error getting instance of userID: ' + UserID
                        log.error(errString)
                        # build AIS error response
                        Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                        Response.error_num = Response.ResponseCodes.INTERNAL_SERVER_ERROR
                        Response.error_str = "AIS.ManageDataResourceSubscription.delete: " + errString
                        defer.returnValue(Response)
                    log.info('Got user resource instance: ' + self.userRes.ResourceIdentity)
                    if  self.ais.AnalyzeTiming != None:
                        log.warning('ManageDataResourceSubscription.delete: got user instance ' + self.ais.TimeStamp())
    
                    # get the user's dispatcher
                    dispatcherID = yield self.__findDispatcher(self.userRes)
                    if (dispatcherID is None):
                        # build AIS error response
                        Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                        Response.error_num = Response.ResponseCodes.NOT_FOUND
                        errString = 'Dispatcher not found for userID' + UserID
                        Response.error_str = "AIS.ManageDataResourceSubscription.delete: " + errString
                        defer.returnValue(Response)
                    else:
                        log.info('FOUND DISPATCHER %s for user %s'%(dispatcherID, UserID))
                        
                    if  self.ais.AnalyzeTiming != None:
                        log.warning('ManageDataResourceSubscription.delete: found dispatcher ' + self.ais.TimeStamp())
                    # delete the workflow
                    Reply = yield self.__deleteDispatcherWorkflow(SubscriptionInfo, dispatcherID)
                    if  self.ais.AnalyzeTiming != None:
                        log.warning('ManageDataResourceSubscription.delete: removed workflow ' + self.ais.TimeStamp())
                    defer.returnValue(Reply)
    
            except ReceivedApplicationError, ex:
                log.info('ManageDataResourceSubscription.delete(): Error attempting to remove Subscription(): %s' %ex.msg_content.MessageResponseBody)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  ex.msg_content.MessageResponseCode
                Response.error_str =  "AIS.ManageDataResourceSubscription.delete: " + ex.msg_content.MessageResponseBody
                defer.returnValue(Response)
        
            except ApplicationError, ex:
                log.info('ManageDataResourceSubscription.delete(): Error attempting to remove Subscription(): %s' %ex)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  "AIS.ManageDataResourceSubscription.delete: " + ex.response_code
                Response.error_str =  str(ex)
                defer.returnValue(Response)

        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(DELETE_SUBSCRIPTION_RSP_TYPE)      
        Response.message_parameters_reference[0].success  = True
        defer.returnValue(Response)

        
    @defer.inlineCallbacks
    def __deleteDispatcherWorkflow(self, SubscriptionInfo, dispatcherID):
 
        log.debug('__deleteDispatcherWorkflow')

        dispatcherRes = yield self.rc.get_instance(dispatcherID)
        (Association, wkflRes) = yield self.__findWorkflowAssociation(dispatcherRes, SubscriptionInfo)
        if Association == None:
            errString = 'Error finding workflow for user ' + SubscriptionInfo.user_ooi_id + \
                        ' and data resource ' + SubscriptionInfo.data_src_id + \
                        ' on dispatcher ' + dispatcherID
            log.error(errString)
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num = Response.ResponseCodes.INTERNAL_SERVER_ERROR
            Response.error_str = errString
            defer.returnValue(Response)
        
        Association.SetNull()
        wkflRes._set_life_cycle_state(wkflRes.RETIRED)
        try:
           yield self.rc.put_resource_transaction([wkflRes])
        except ApplicationError, ex:
            log.info('ManageDataResourceSubscription.__deleteDispatcherWorkflow(): Error attempting to retire workflow & association: %s' %ex)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)
        
        #
        # Create the dispatcher workflow for delete event  #
        dwfRes = yield self.rc.create_instance(DISPATCHER_WORKFLOW_RESOURCE_TYPE, ResourceName = 'Delete DispatcherWorkflow')
        dwfRes.dataset_id = SubscriptionInfo.data_src_id
        dwfRes.workflow_path = SubscriptionInfo.dispatcher_script_path
        # Publish the delete subscription notification

        if self.pfd is None:
            pubfact = PublisherFactory(publisher_type=DelSubscriptionEventPublisher, process=self.ais)
            self.pfd = yield pubfact.build()

        yield self.pfd.create_and_publish_event(dispatcher_workflow = dwfRes.ResourceObject, origin = dispatcherID)

        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(DELETE_SUBSCRIPTION_RSP_TYPE)      
        Response.message_parameters_reference[0].success  = True
        defer.returnValue(Response)


    @defer.inlineCallbacks
    def __findWorkflowAssociation(self, dispatcherRes, SubscriptionInfo):

        log.debug('__findWorkflowAssociation')
        # get the dispatcher's associations
        Associations = yield self.ac.find_associations(subject=dispatcherRes)       
        log.debug('Found ' + str(len(Associations)) + ' associations for dispatcher ' + dispatcherRes.ResourceIdentity)
               
        # search for workflow with same userID and dataResourceID as in subscription
        for Association in Associations:
            log.debug('Asso = \n'+str(Association))
            try:
                log.debug('getting '+str(Association.ObjectReference.key))
                Ref = yield self.rc.get_instance(Association.ObjectReference.key)
            except ResourceClientError:
                log.error('Error getting instance of Resource: ' + Association.ObjectReference.key)
                continue
            if log.getEffectiveLevel() <= logging.DEBUG:
                log.debug('Ref = \n'+str(Ref))
            if Ref.ResourceObjectType != DISPATCHER_WORKFLOW_RESOURCE_TYPE:
                continue
            if ((Ref.user_ooi_id == SubscriptionInfo.user_ooi_id) and
                (Ref.dataset_id == SubscriptionInfo.data_src_id)):
                defer.returnValue([Association, Ref])
        defer.returnValue([None, None])
        

    @defer.inlineCallbacks
    def find(self, msg):
        """
        @brief find all subscriptions for a data resource
        @param msg GPB, 
        @GPB{Input,9203,1}
        @GPB{Returns,9204,1}
        @retval success
        """
        log.info('ManageDataResourceSubscription.findDataResourceSubscriptions()\n')

        try:
            log.debug('find: Calling NAS.getSubscriptionList service')
            reply = yield self.nac.getSubscriptionList(msg)
            numSubsReturned = len(reply.message_parameters_reference[0].subscriptionListResults)
            log.debug('getSubscriptionList returned: ' + str(numSubsReturned) + ' subscriptions.')
        except ReceivedApplicationError, ex:
            log.info('AIS.ManageDataResourceSubscription.find(): Error calling NAS.getSubscriptionList: %s' %ex)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  ex.msg_content.MessageResponseBody
            defer.returnValue(Response)

        #
        # Instantiate a bounds object, and load it up with the given bounds
        # info
        #
        bounds = SpatialTemporalBounds()
        bounds.loadBounds(msg.message_parameters_reference.dataBounds)

        # create response message
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        respMsg.message_parameters_reference.add()
        respMsg.message_parameters_reference[0] = respMsg.CreateObject(GET_SUBSCRIPTION_LIST_RESP_TYPE)

        #
        # Now iterate through the list, filtering by the bounds.  If no metadata
        # is found, log an error (shouldn't happen)
        #
        j = 0
        for result in reply.message_parameters_reference[0].subscriptionListResults:
            dSetResID = result.datasetMetadata.data_resource_id
            dSetMetadata = yield self.metadataCache.getDSetMetadata(dSetResID)
            if dSetMetadata is None:
                log.error('Metadata not found for dataset: %s' %(dSetResID))
            else:
                log.debug('Metadata found for dataset: %s' %(dSetResID))
                if bounds.isInBounds(dSetMetadata):
                    respMsg.message_parameters_reference[0].subscriptionListResults.add()
                    self.__loadSubscriptionListResultsMsg(respMsg.message_parameters_reference[0].subscriptionListResults[j], result)
                    j = j + 1
                
        defer.returnValue(respMsg)


    def __loadSubscriptionListResultsMsg(self, respMsg, result):
        #
        # Private utility method to build a subscription list response message.
        # 
        respMsg.subscriptionInfo.user_ooi_id = result.subscriptionInfo.user_ooi_id
        respMsg.subscriptionInfo.data_src_id = result.subscriptionInfo.data_src_id
        respMsg.subscriptionInfo.subscription_type = result.subscriptionInfo.subscription_type
        respMsg.subscriptionInfo.email_alerts_filter = result.subscriptionInfo.email_alerts_filter
        respMsg.subscriptionInfo.dispatcher_alerts_filter = result.subscriptionInfo.dispatcher_alerts_filter
        respMsg.subscriptionInfo.dispatcher_script_path = result.subscriptionInfo.dispatcher_script_path
        respMsg.subscriptionInfo.date_registered = result.subscriptionInfo.date_registered

        respMsg.datasetMetadata.user_ooi_id = result.datasetMetadata.user_ooi_id
        respMsg.datasetMetadata.data_resource_id = result.datasetMetadata.data_resource_id
        respMsg.datasetMetadata.title = result.datasetMetadata.title
        respMsg.datasetMetadata.institution = result.datasetMetadata.institution
        respMsg.datasetMetadata.source = result.datasetMetadata.source
        respMsg.datasetMetadata.references = result.datasetMetadata.references
        respMsg.datasetMetadata.summary = result.datasetMetadata.summary
        respMsg.datasetMetadata.conventions = result.datasetMetadata.conventions
        respMsg.datasetMetadata.comment = result.datasetMetadata.comment
        respMsg.datasetMetadata.ion_time_coverage_start = result.datasetMetadata.ion_time_coverage_start
        respMsg.datasetMetadata.ion_time_coverage_end = result.datasetMetadata.ion_time_coverage_end
        respMsg.datasetMetadata.ion_geospatial_lat_min = result.datasetMetadata.ion_geospatial_lat_min
        respMsg.datasetMetadata.ion_geospatial_lat_max = result.datasetMetadata.ion_geospatial_lat_max
        respMsg.datasetMetadata.ion_geospatial_lon_min = result.datasetMetadata.ion_geospatial_lon_min
        respMsg.datasetMetadata.ion_geospatial_lon_max = result.datasetMetadata.ion_geospatial_lon_max
        respMsg.datasetMetadata.ion_geospatial_vertical_min = result.datasetMetadata.ion_geospatial_vertical_min
        respMsg.datasetMetadata.ion_geospatial_vertical_max = result.datasetMetadata.ion_geospatial_vertical_max
        respMsg.datasetMetadata.ion_geospatial_vertical_positive = result.datasetMetadata.ion_geospatial_vertical_positive
        respMsg.datasetMetadata.download_url = result.datasetMetadata.download_url
        

    @defer.inlineCallbacks
    def _CheckRequest(self, request):
        # Check for correct request protocol buffer type
        if request.MessageType != AIS_REQUEST_MSG_TYPE:
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = 'Bad message type receieved, ignoring'
            defer.returnValue(Response)

        # Check payload in message
        if not request.IsFieldSet('message_parameters_reference'):
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
            Response.error_num = Response.ResponseCodes.BAD_REQUEST
            Response.error_str = "Required field [message_parameters_reference] not found in message"
            defer.returnValue(Response)
  
        defer.returnValue(None)
