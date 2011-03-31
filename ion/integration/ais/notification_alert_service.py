__author__ = 'mauricemanning'
#!/usr/bin/env python

"""
@file ion/integration/notification_alert_service.py
@author Maurice Manning
@brief Utility service to send email alerts for user subscriptions
"""

import sys
import traceback

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

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import SUBSCRIPTION_INFO_TYPE

class NotificationAlertError(ApplicationError):
    """
    An Exception class for the hello errors example
    It inherits from the Application Error. Give a 'reason' and a 'response_code'
    when throwing a ApplicationError!
    """

class NotificationAlertService(ServiceProcess):
    """
    Service to provide clients access to backend data
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='notification_alert',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        log.debug('NotificationAlertService.__init__() start')
        ServiceProcess.__init__(self, *args, **kwargs)

        self.rc = ResourceClient(proc = self)
        self.mc = MessageClient(proc = self)
        self.store = AttributeStoreClient(proc = self)

        #create the process to read the queues
        log.debug('NotificationAlertService.__init__()')

    def slc_init(self):
        pass

    @defer.inlineCallbacks
    def op_addSubscription(self, content, headers, msg):
        """
        @brief Add a new subscription to the set of queue to monitor
        @param userId, queueId
        @retval none
        """
        log.info('op_addSubscription')

        # Check only the type received and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s')

        yield self.store.put(content.message_parameters_reference.user_ooi_id, content.message_parameters_reference.queue_id)

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result = respMsg.ResponseCodes.OK;

        log.info('op_addSubscription finished')
        yield self.reply_ok(msg, respMsg)

    @defer.inlineCallbacks
    def op_removeSubscription(self, content, headers, msg):
        """
        @brief remove a subscription from the set of queues to monitor
        @param userId, queueId
        @retval none
        """
        log.debug('op_removeSubscription: \n'+str(content))
       # Check only the type received and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            # This will terminate the  service. As an alternative reply okay with an error message
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s'
                                     % str(content))

        log.debug('Removing queue_id %s from store...' % content.message_parameters_reference.queue_id)
        yield self.store.remove(content.message_parameters_reference.queue_id)
        log.debug('Removal completed')

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='NAS Add Subscription result')
        respMsg.result = respMsg.ResponseCodes.OK

        log.info('op_removeSubscription finished')
        yield self.reply_ok(msg, respMsg)


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

