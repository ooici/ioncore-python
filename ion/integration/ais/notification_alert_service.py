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
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.object import object_utils
from ion.core.exception import ApplicationError
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.attributestore import AttributeStoreClient
from ion.core.messaging import messaging
from ion.services.dm.distribution.events import ResourceLifecycleEventSubscriber
from ion.core.process.process import Process
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

# Import smtplib for the actual sending function
import smtplib
from smtplib import SMTPException
import string



# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import SUBSCRIPTION_INFO_TYPE
#from ion.integration.ais.ais_object_identifiers import RESOURCE_CFG_REQUEST_TYPE
RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)


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

        self.rc =    ResourceClient(proc = self)
        self.mc =    MessageClient(proc = self)
        self.store = AttributeStoreClient(proc = self)

    def slc_init(self):
        pass

    @defer.inlineCallbacks
    def op_foobar(msg, content):
        log.info('NotificationAlertService.foobar notification event received ')

    """
    @defer.inlineCallbacks
    def handle_event(msg, content):
        log.info('NotificationAlertService.handle_event notification event received ')

        #Parse notification to determine the data src source


        #Find users that are interested in that data source


        # get the user information from the Identity Registry
        # build the Identity Registry request for get_user message
        Request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
        Request.configuration = Request.CreateObject(USER_OOIID_TYPE)
        Request.configuration.ooi_id = content.message_parameters_reference.user_ooi_id

        try:
            user_info = yield self.irc.get_user(Request)
        except ReceivedApplicationError, ex:
            self.fail(" NotificationAlertService.handle_event get_user failed to find the user [%s]"%msg.message_parameters_reference.user_ooi_id)
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS updateUserEmail error response')
            Response.error_num = ex.msg_content.MessageResponseCode
            Response.error_str = ex.msg_content.MessageResponseBody
            defer.returnValue(Response)
        log.info('NotificationAlertService.handle_event get_user result: ' + str(user_info))


        # Send the message via our own SMTP server, but don't include the envelope header.
        # Create the container (outer) email message.
        FROM = 'mmmanning@ucsd.edu'
        TO = 'mmmanning@ucsd.edu'

        SUBJECT = "OOI CI Data source notification Alert"

        BODY = "You have subscribed to data set XX in OOI CI. This email is a notification alert that ..."

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
        except SMTPException:
            log.info('NotificationAlertService.handle_event Error: unable to send email')

        """


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

        # check that ooi_id is present in GPB
        if not content.message_parameters_reference.IsFieldSet('user_ooi_id'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [user_ooi_id] not found in message"
             defer.returnValue(Response)

        # check that exchange point name is present in GPB
        if not content.message_parameters_reference.IsFieldSet('exchange_point'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [exchange_point] not found in message"
             defer.returnValue(Response)

        # check that routing key name is present in GPB
        if not content.message_parameters_reference.IsFieldSet('routing_key'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [routing_key] not found in message"
             defer.returnValue(Response)


        def handle_event(content):
            log.info('NotificationAlertService.handle_event notification event received ')

            # Send the message via our own SMTP server, but don't include the envelope header.
            # Create the container (outer) email message.
            FROM = 'mmmanning@ucsd.edu'
            TO = 'mmmanning@ucsd.edu'

            SUBJECT = "OOI CI Data source notification Alert"

            BODY = "You have subscribed to data set XX in OOI CI. This email is a notification alert that ..."

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
            except SMTPException:
                log.info('NotificationAlertService.handle_event Error: unable to send email')
            log.info('NotificationAlertService.handle_event completed ')



        #Check if this XPoint is already registered
        map = yield self.store.query(content.message_parameters_reference.exchange_point)
        #If no - add new entry to attrStore
        if (len(map) == 0):
            #exchange_point is new, first create a map for this routing key, add this user as the first in the list
            log.info('NotificationAlertService.op_addSubscription exchange_point is new')
            newBKlist = [content.message_parameters_reference.user_ooi_id]
            newBRKMap = [content.message_parameters_reference.routing_key, newBKlist]
            #add this map to the set of binding keys for this topic
            self.store.put(content.message_parameters_reference.exchange_point, newBRKMap)
        else:
            #If yes - get the map for this binding key
            log.info('NotificationAlertService.op_addSubscription exchange_point exists')
            bklist = yield self.store.query(content.message_parameters_reference.routing_key)
            if (len(bklist) == 0):
                #create a map for this binding key, add this user as the first in the list
                newBKlist = [content.message_parameters_reference.user_ooi_id]
                newBRKMap = dict({content.message_parameters_reference.routing_key:newBKlist})
            else:
                #the topic can binding key map[s oth exist, just add the user to the list
                bklist.append(content.message_parameters_reference.user_ooi_id)
            bklist.append(content.message_parameters_reference.user_ooi_id)
            self.store.put(content.message_parameters_reference.exchange_point, list)
            
        # listen for resource updates on resource UUID
        log.info('NotificationAlertService.op_addSubscription create ResourceLifecycleEventSubscriber')
        self.sub = ResourceLifecycleEventSubscriber(process=self, origin="magnet_topic")     #origin=content.message_parameters_reference.exchange_point)
        log.info('NotificationAlertService.op_addSubscription set handler for ResourceLifecycleEventSubscriber')
        self.sub.ondata = handle_event    # need to do something with the data when it is received
        log.info('NotificationAlertService.op_addSubscription register and activate ResourceLifecycleEventSubscriber')
        yield self.sub.register()
        yield self.sub.initialize()
        yield self.sub.activate()
        log.info('NotificationAlertService.op_addSubscription activation complete')

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result = respMsg.ResponseCodes.OK;

        log.info('NotificationAlertService.op_addSubscription complete')
        yield self.reply_ok(msg, respMsg)

    @defer.inlineCallbacks
    def op_removeSubscription(self, content, headers, msg):
        """
        @brief remove a subscription from the set of queues to monitor
        @param userId, exchange_point, binding_key
        @retval none
        """
        log.debug('NotificationAlertService.op_removeSubscription: \n'+str(content))

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

        # check that exchange point name is present in GPB
        if not content.message_parameters_reference.IsFieldSet('exchange_point'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [exchange_point] not found in message"
             defer.returnValue(Response)

        # check that binding key name is present in GPB
        if not content.message_parameters_reference.IsFieldSet('routing_key'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [routing_key] not found in message"
             defer.returnValue(Response)



        log.info('Removing exchange_point %s from store...' % content.message_parameters_reference.exchange_point)
        yield self.store.remove(content.message_parameters_reference.exchange_point)
        log.debug('Removal completed')

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result = respMsg.ResponseCodes.OK

        log.info('NotificationAlertService..op_removeSubscription complete')
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


# Spawn of the process using the module name
factory = ProcessFactory(NotificationAlertService)

