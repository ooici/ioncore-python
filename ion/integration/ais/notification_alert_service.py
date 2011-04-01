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
from ion.services.dm.distribution.publisher_subscriber import SubscriberFactory, Subscriber
from ion.core.process.process import Process
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

# Import smtplib for the actual sending function
import smtplib
from smtplib import SMTPException
import string



# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import SUBSCRIPTION_INFO_TYPE
from ion.integration.ais.ais_object_identifiers import RESOURCE_CFG_REQUEST_TYPE


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
        if not content.message_parameters_reference.IsFieldSet('xpoint'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [xpoint] not found in message"
             defer.returnValue(Response)

        # check that binding key name is present in GPB
        if not content.message_parameters_reference.IsFieldSet('binding_key'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [xpoint] not found in message"
             defer.returnValue(Response)


        proc = Process()
        yield proc.spawn()

        msgs = []
        def handle_msg(content):
            msgs.append(content['content'])

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
                self.fail("get_user failed to find the user [%s]"%msg.message_parameters_reference.user_ooi_id)
                # build AIS error response
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS updateUserEmail error response')
                Response.error_num = ex.msg_content.MessageResponseCode
                Response.error_str = ex.msg_content.MessageResponseBody
                defer.returnValue(Response)


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
               print "Successfully sent email"
            except SMTPException:
               print "Error: unable to send email"



        #Check if this XPoint is already registered
        map = yield self.store.query(content.message_parameters_reference.xpoint)
        #If no - add new entry to attrStore
        if (len(tlist) == 0):
            #XPoint is new, first create a map for this binding key, add this user as the first in the list
            newBKlist = [content.message_parameters_reference.user_ooi_id]
            newBRKMap = {content.message_parameters_reference.binding_key, newBKlist}
            #add this map to the set of binding keys for this topic
            self.store.put(content.message_parameters_reference.xpoint, newBRKMap)
            #Create a listener for this topic / binding_key combination
            sf = SubscriberFactory(xp_name="magnet.topic", process=proc)
            sub = yield sf.build(binding_key='arf_test', handler=handle_msg)

        else:
            #If yes - get the map for this binding key
            bklist = yield self.store.query(content.message_parameters_reference.binding_key)
            if (len(tlist) == 0):
                #create a map for this binding key, add this user as the first in the list
                newBKlist = [content.message_parameters_reference.user_ooi_id]
                newBRKMap = {content.message_parameters_reference.binding_key, newBKlist}

                #Create a listener for this topic / binding_key combination
                sf = SubscriberFactory(xp_name="magnet.topic", process=proc)
                sub = yield sf.build(binding_key='arf_test', handler=handle_msg)
            else:
                #the topic can binding key map[s oth exist, just add the user to the list
                bklist.append(content.message_parameters_reference.user_ooi_id)

            
            tlist.append(content.message_parameters_reference.user_ooi_id)
            self.store.put(content.message_parameters_reference.xpoint, list)
            






        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result= 'success';

        log.info('NotificationAlertService.op_addSubscription complete')
        yield self.reply_ok(msg, respMsg)

    @defer.inlineCallbacks
    def op_removeSubscription(self, content, headers, msg):
        """
        @brief remove a subscription from the set of queues to monitor
        @param userId, xpoint, binding_key
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
        if not content.message_parameters_reference.IsFieldSet('xpoint'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [xpoint] not found in message"
             defer.returnValue(Response)

        # check that binding key name is present in GPB
        if not content.message_parameters_reference.IsFieldSet('binding_key'):
             # build AIS error response
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
             Response.error_num = Response.ResponseCodes.BAD_REQUEST
             Response.error_str = "Required field [xpoint] not found in message"
             defer.returnValue(Response)



        log.info('Removing xpoint %s from store...' % content.message_parameters_reference.xpoint)
        yield self.store.remove(content.message_parameters_reference.xpoint)
        log.debug('Removal completed')

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result= 'success';

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

