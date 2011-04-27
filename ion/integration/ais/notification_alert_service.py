__author__ = 'mauricemanning'
#!/usr/bin/env python

"""
@file ion/integration/notification_alert_service.py
@author Maurice Manning
@brief Utility service to send email alerts for user subscriptions
"""

import sys
import traceback

from ion.core import ioninit
CONF = ioninit.config(__name__)

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.object import object_utils
import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory

import string
import smtplib

from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.attributestore import AttributeStoreClient
from ion.services.coi.identity_registry import IdentityRegistryClient
from ion.integration.ais.app_integration_service import AppIntegrationServiceClient

from ion.core.exception import ReceivedApplicationError, ReceivedContainerError
from ion.core.data.store import Query
from ion.services.dm.distribution.events import DatasetSupplementAddedEventSubscriber


from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       SUBSCRIPTION_INFO_TYPE, \
                                                       REGISTER_USER_REQUEST_TYPE, \
                                                       UPDATE_USER_PROFILE_REQUEST_TYPE, \
                                                       REGISTER_USER_RESPONSE_TYPE, \
                                                       GET_SUBSCRIPTION_LIST_REQ_TYPE, \
                                                       GET_SUBSCRIPTION_LIST_RESP_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_RSP_TYPE, \
                                                       DELETE_SUBSCRIPTION_REQ_TYPE, \
                                                       DELETE_SUBSCRIPTION_RSP_TYPE

RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
USER_OOIID_TYPE = object_utils.create_type_identifier(object_id=1403, version=1)


class NotificationAlertService(ServiceProcess):
    """
    Service to provide clients access to backend data
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='notification_alert',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        log.debug('NotificationAlertService.__init__()')
        ServiceProcess.__init__(self, *args, **kwargs)

        #self.rc =    ResourceClient(proc = self)
        self.mc =    MessageClient(proc = self)
        self.irc =   IdentityRegistryClient(proc = self)
        self.store = AttributeStoreClient(proc = self)
        self.aisc =  AppIntegrationServiceClient(proc = self)

        #initialize index store for subscription information
        SUBSCRIPTION_INDEXED_COLUMNS = ['user_ooi_id', 'data_src_id', 'subscription_type', 'email_alerts_filter', 'dispatcher_alerts_filter', 'dispatcher_script_path']
        index_store_class_name = self.spawn_args.get('index_store_class', CONF.getValue('index_store_class', default='ion.core.data.store.IndexStore'))
        self.index_store_class = pu.get_class(index_store_class_name)
        self.index_store = self.index_store_class(self, indices=SUBSCRIPTION_INDEXED_COLUMNS )


    def slc_init(self):
        pass


    @defer.inlineCallbacks
    def handle_offline_event(self, content):
        log.info('NotificationAlertService.handle_offline_event notification event received ')
        #Check that the item is in the store
        log.info('NotificationAlertService.handle_event content   : %s', content)
         

    @defer.inlineCallbacks
    def handle_update_event(self, content):
            log.info('NotificationAlertService.handle_event notification event received')
            #Check that the item is in the store
            log.info('NotificationAlertService.handle_event content   : %s', content)

            #log.info('NotificationAlertService.handle_event props  : %s', content['content'])
            msg = content['content'];
            #log.info('NotificationAlertService.handle_event additional_data  : %s', msg.additional_data)

            query = Query()
            query.add_predicate_eq('data_src_id', msg.additional_data.datasource_id)
            rows = yield self.index_store.query(query)
            log.info("NotificationAlertService.handle_update_event  Rows returned %s " % (rows,))

            #add each result row into the response message
            i = 0
            for key, row in rows.iteritems ( ) :
                log.info("NotificationAlertService.op_getSubscriptionList  First row data set id %s", rows[key]['data_src_id'] )


                tempTbl = {}
                # get the user information from the Identity Registry
                yield self.GetUserInformation(rows[key]['user_ooi_id'], tempTbl)
                log.info('NotificationAlertService.handle_update_event user email: %s', tempTbl['user_email'] )

                #enum SubscriptionType {EMAIL = 0;  DISPATCHER = 1;  EMAILANDDISPATCHER = 2; }
                #enum AlertsFilter { UPDATES = 0;  METADATACHENGE = 1;  DATASOURCEOFFLINE = 2; }

                #rows[key]['subscription_type'] == SUBSCRIPTION_INFO_TYPE.subscription_type.EMAIL

                if (rows[key]['subscription_type'] == 2 or rows[key]['subscription_type'] == 0 ) \
                    and rows[key]['email_alerts_filter'] == 0 :
                    # Send the message via our own SMTP server, but don't include the envelope header.
                    # Create the container (outer) email message.
                    FROM = 'OOI@ucsd.edu'
                    TO = tempTbl['user_email']

                    SUBJECT = "OOI CI Data Alert"

                    BODY = string.join("You have subscribed to data set %s" % msg.additional_data.datasource_id)
                    #                    "in OOI CI. This is an alert that additional data has been received. \r\n Data Source Title: %s" %  msg.additional_data.title)


                    body = string.join((


                        "From: %s" % FROM,
                        "To: %s" % TO,
                        "Subject: %s" % SUBJECT,
                        "",
                        BODY), "\r\n")


                    try:
                       smtpObj = smtplib.SMTP('mail.oceanobservatories.org', 25, 'localhost')
                       smtpObj.sendmail(FROM, [TO], body)
                       log.info('NotificationAlertService.handle_event Successfully sent email' )
                    except smtplib.SMTPException:
                        log.info('NotificationAlertService.handle_event Error: unable to send email')
                    log.info('NotificationAlertService.handle_event completed ')


    @defer.inlineCallbacks
    def op_addSubscription(self, content, headers, msg):
        """
        @brief Add a new subscription to the set of queue to monitor
        @param userId, queueId
        @retval none
        """
        log.info('NotificationAlertService.op_addSubscription()\n ')
        yield self.CheckRequest(content)

        # Check only the type received
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s')

        # check that subscriptionInfo is present in GPB
        if not content.message_parameters_reference.IsFieldSet('subscriptionInfo'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [subscriptionInfo] not found in message"
             defer.returnValue(Response)

        # check that ooi_id is present in GPB
        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('user_ooi_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [user_ooi_id] not found in message"
             defer.returnValue(Response)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('data_src_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [data_src_id] not found in message"
             defer.returnValue(Response)

        # check that subscription type enum is present in GPB
        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('subscription_type'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [subscription_type] not found in message"
             defer.returnValue(Response)

        # get the data resource metadata information
        #yield self.GetDatasetInformation(content.message_parameters_reference.data_src_id, attributes)


        #add the subscription to the index store
        self.attributes = {'user_ooi_id':content.message_parameters_reference.subscriptionInfo.user_ooi_id,
                   'data_src_id': content.message_parameters_reference.subscriptionInfo.data_src_id,
                   'subscription_type':content.message_parameters_reference.subscriptionInfo.subscription_type,
                   'email_alerts_filter': content.message_parameters_reference.subscriptionInfo.email_alerts_filter,
                   'dispatcher_alerts_filter':content.message_parameters_reference.subscriptionInfo.dispatcher_alerts_filter,
                   'dispatcher_script_path': content.message_parameters_reference.subscriptionInfo.dispatcher_script_path
        }
        log.info('NotificationAlertService.op_addSubscription attributes userid: %s', content.message_parameters_reference.subscriptionInfo.user_ooi_id )
        log.info('NotificationAlertService.op_addSubscription attributes datasrc id: %s', content.message_parameters_reference.subscriptionInfo.data_src_id )
        self.keyval = content.message_parameters_reference.subscriptionInfo.data_src_id + content.message_parameters_reference.subscriptionInfo.user_ooi_id
        log.info('NotificationAlertService.op_addSubscription attributes keyval id: %s', self.keyval )
        yield self.index_store.put(self.keyval , self.keyval, self.attributes)

        # Create the correct listener for this data source

        #First, check if updates should be subscribed to for this data source
        log.info('NotificationAlertService.op_addSubscription create DatasetSupplementAddedEventSubscriber')
        if ( content.message_parameters_reference.subscriptionInfo.subscription_type == content.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER and (content.message_parameters_reference.subscriptionInfo.email_alerts_filter == content.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES or content.message_parameters_reference.subscriptionInfo.dispatcher_alerts_filter == content.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES) ) \
            or (content.message_parameters_reference.subscriptionInfo.subscription_type == content.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAIL and content.message_parameters_reference.subscriptionInfo.email_alerts_filter == content.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES) \
            or (content.message_parameters_reference.subscriptionInfo.subscription_type == content.message_parameters_reference.subscriptionInfo.SubscriptionType.DISPATCHER and content.message_parameters_reference.subscriptionInfo.dispatcher_alerts_filter == content.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES):
            
            self.sub = DatasetSupplementAddedEventSubscriber(process=self, origin="magnet_topic")
            log.info('NotificationAlertService.op_addSubscription set handler for DatasetSupplementAddedEventSubscriber')
            self.sub.ondata = self.handle_update_event    # need to do something with the data when it is received
            yield self.sub.register()
            yield self.sub.initialize()
            yield self.sub.activate()
            log.info('NotificationAlertService.op_addSubscription DatasetSupplementAddedEvent activation complete')

        """
        #Second, check if data source unavailable events should be subscribed to for this data source
        log.info('NotificationAlertService.op_addSubscription create DatasourceUnavailableEventSubscriber')
        if ( content.message_parameters_reference.subscription_type == content.message_parameters_reference.SubscriptionType.EMAILANDDISPATCHER and (content.message_parameters_reference.email_alerts_filter == content.message_parameters_reference.AlertsFilter.DATASOURCEOFFLINE or content.message_parameters_reference.dispatcher_alerts_filter == content.message_parameters_reference.AlertsFilter.DATASOURCEOFFLINE) ) \
            or (content.message_parameters_reference.subscription_type == content.message_parameters_reference.SubscriptionType.EMAIL and content.message_parameters_reference.email_alerts_filter == content.message_parameters_reference.AlertsFilter.DATASOURCEOFFLINE) \
            or (content.message_parameters_reference.subscription_type == content.message_parameters_reference.SubscriptionType.DISPATCHER and content.message_parameters_reference.dispatcher_alerts_filter == content.message_parameters_reference.AlertsFilter.DATASOURCEOFFLINE):
            log.info('NotificationAlertService.op_addSubscription set handler for DatasourceUnavailableEventSubscriber NOT IMPL YET!!!!!!!')
        """

        # create the register_user response GPBs
        log.info('NotificationAlertService.op_addSubscription construct response message')
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result = respMsg.ResponseCodes.OK;

        log.info('NotificationAlertService.op_addSubscription complete')
        yield self.reply_ok(msg, respMsg)

    @defer.inlineCallbacks
    def op_removeSubscription(self, content, headers, msg):
        """
        @brief remove a subscription from the set of queues to monitor
        @param
        @retval none
        """
        log.debug('NotificationAlertService.op_removeSubscription: \n'+str(content))

        # Check only the type received
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s'% str(content))

        # check that subscriptionInfo is present in GPB
        if not content.message_parameters_reference.IsFieldSet('subscriptionInfo'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [subscriptionInfo] not found in message"
             defer.returnValue(Response)

        # check that ooi_id is present in GPB
        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('user_ooi_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [user_ooi_id] not found in message"
             defer.returnValue(Response)

        # check that data_src_id name is present in GPB
        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('data_src_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [data_src_id] not found in message"
             defer.returnValue(Response)

        log.info('NotificationAlertService.op_removeSubscription  Removing subscription %s from store...', content.message_parameters_reference.subscriptionInfo.data_src_id)

        self.keyval = content.message_parameters_reference.subscriptionInfo.data_src_id + content.message_parameters_reference.subscriptionInfo.user_ooi_id
        log.info("NotificationAlertService.op_removeSubscription key: %s ", self.keyval)
        #rc = yield self.index_store.get(self.keyval)
        yield self.index_store.remove(self.keyval)

        log.info('NotificationAlertService.op_removeSubscription  Removal completed')

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result = respMsg.ResponseCodes.OK

        log.info('NotificationAlertService..op_removeSubscription complete')
        yield self.reply_ok(msg, respMsg)



    @defer.inlineCallbacks
    def op_getSubscriptionList(self, content, headers, msg):
        """
        @brief remove a subscription from the set of queues to monitor
        @param
        @retval none
        """
        log.debug('NotificationAlertService.op_getSubscriptionList \n'+str(content))

        # Check only the type received
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s'% str(content))

        # check that ooi_id is present in GPB
        if not content.message_parameters_reference.IsFieldSet('user_ooi_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [user_ooi_id] not found in message"
             defer.returnValue(Response)

        #Check that the item is in the store
        query = Query()
        query.add_predicate_eq('user_ooi_id', content.message_parameters_reference.user_ooi_id)
        rows = yield self.index_store.query(query)
        log.info("NotificationAlertService.op_getSubscriptionList  Rows returned %s " % (rows,))

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.message_parameters_reference.add()
        respMsg.message_parameters_reference[0] = respMsg.CreateObject(GET_SUBSCRIPTION_LIST_RESP_TYPE)

        #add each result row into the response message
        i = 0
        for key, row in rows.iteritems ( ) :
            log.info("NotificationAlertService.op_getSubscriptionList  First row data set id %s", rows[key]['data_src_id'] )
            respMsg.message_parameters_reference[0].subscriptionInfo.add()
            respMsg.message_parameters_reference[0].subscriptionInfo[i].user_ooi_id = rows[key]['user_ooi_id']
            respMsg.message_parameters_reference[0].subscriptionInfo[i].data_src_id = rows[key]['data_src_id']
            respMsg.message_parameters_reference[0].subscriptionInfo[i].subscription_type = rows[key]['subscription_type']
            respMsg.message_parameters_reference[0].subscriptionInfo[i].email_alerts_filter = rows[key]['email_alerts_filter']
            respMsg.message_parameters_reference[0].subscriptionInfo[i].dispatcher_alerts_filter = rows[key]['dispatcher_alerts_filter']
            respMsg.message_parameters_reference[0].subscriptionInfo[i].dispatcher_script_path = rows[key]['dispatcher_script_path']

        respMsg.result = respMsg.ResponseCodes.OK

        log.info('NotificationAlertService.op_getSubscriptionList complete')
        yield self.reply_ok(msg, respMsg)


    @defer.inlineCallbacks
    def CheckRequest(self, request):
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

    @defer.inlineCallbacks
    def GetUserInformation(self, user_ooi_id, tempTbl):

        log.info('NotificationAlertService.GetUserInformation user:  %s  attributes: %s', user_ooi_id, tempTbl)
        #Check that the user exists in the Identity Registry and save email address
        # build the Identity Registry request for get_user message
        Request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
        Request.configuration = Request.CreateObject(USER_OOIID_TYPE)
        Request.configuration.ooi_id = user_ooi_id

        # get the user information from the Identity Registry
        try:
            user_info = yield self.irc.get_user(Request)
        except ReceivedApplicationError, ex:
             # build AIS error response

             log.info('NotificationAlertService.GetUserInformation Send Error: %s ', ex.msg_content.MessageResponseBody)
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS Notification Alert Service: Add Subscription error response')
             Response.error_num = ex.msg_content.MessageResponseCode
             Response.error_str = ex.msg_content.MessageResponseBody
             defer.returnValue(Response)

        tempTbl['user_email'] = user_info.resource_reference.email
        log.info('NotificationAlertService.GetUserInformation user email: %s', user_info.resource_reference.email)
        #Request.configuration.email = msg.message_parameters_reference.email_address

        defer.returnValue(None)

    """
    def __printSubscription(self, attributes):
        subAttrs = list(attributes)
        log.info('Subscription Attributes: ')
        for attr in subAttrs:
            log.debug('   %s = %s'  % (attr, attributes[attr]))

    @defer.inlineCallbacks
    def GetDatasetInformation(self, data_src_id, attributes):
        #
        # Create a request message to get the metadata details about the
        # source (i.e., where the dataset came from) of a particular dataset
        # resource ID.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.data_resource_id = data_src_id

        log.debug('NotificationAlertService.op_addSubscription Calling getDataResourceDetail.')
        rspMsg = yield self.aisc.getDataResourceDetail(reqMsg)
        log.debug('NotificationAlertService.op_addSubscription getDataResourceDetail returned:\n' + \
            str('resource_id: ') + \
            str(rspMsg.message_parameters_reference[0].data_resource_id) + \
            str('\n'))

        dSource = rspMsg.message_parameters_reference[0]
        attributes['data_resource_id'] = dSource.data_resource_id

        defer.returnValue(None)
    """




class NotificationAlertServiceClient(ServiceClient):
    """
    This is a service client for NotificationAlertService.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "notification_alert"
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def addSubscription(self, message):
        yield self._check_init()
        log.debug('NAS_client.addSubscription: sending following message to addSubscription:\n%s' % str(message))
        (content, headers, payload) = yield self.rpc_send('addSubscription', message)
        log.debug('NAS_client.addSubscription: reply:\n' + str(content))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def removeSubscription(self, message):
        yield self._check_init()
        log.debug('NAS_client.removeSubscription: sending following message to removeSubscription:\n%s' % str(message))
        (content, headers, payload) = yield self.rpc_send('removeSubscription', message)
        log.debug('NAS_client.removeSubscription: reply:\n' + str(content))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def getSubscriptionList(self, message):
        yield self._check_init()
        log.debug('NAS_client.getSubscriptionList: sending following message to getSubscriptionList:\n%s' % str(message))
        (content, headers, payload) = yield self.rpc_send('getSubscriptionList', message)
        log.debug('NAS_client.getSubscriptionList: reply:\n' + str(content))
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(NotificationAlertService)