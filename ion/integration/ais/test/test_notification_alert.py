#!/usr/bin/env python

"""
@file ion/integration/test_notification_alert.py
@test ion.integration.notification_alert_service
@author Maurice Manning
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from twisted.internet import defer

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.test.iontest import IonTestCase
from ion.core.messaging import messaging
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG

from ion.core.object import object_utils
from ion.core.data import store

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import SUBSCRIPTION_INFO_TYPE
from ion.integration.ais.ais_object_identifiers import  GET_SUBSCRIPTION_LIST_REQ_TYPE, GET_SUBSCRIPTION_LIST_RESP_TYPE

# Create CDM Type Objects
SUBSCRIPTION_INFO_TYPE = object_utils.create_type_identifier(object_id=9201, version=1)


class NotificationAlertTest(IonTestCase):
    """
    Testing Notification Alert Service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                    'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}},
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },
            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },
            {
                'name':'attributestore',
                'module':'ion.services.coi.attributestore',
                'class':'AttributeStoreService'
            },
            {
                'name':'identity_registry',
                'module':'ion.services.coi.identity_registry',
                'class':'IdentityRegistryService'
            },
            {
                'name':'store_service',
                'module':'ion.core.data.store_service',
                'class':'StoreService'
            },
            {
                'name':'notification_alert',
                'module':'ion.integration.ais.notification_alert_service',
                'class':'NotificationAlertService'
            },

            ]        

        sup = yield self._spawn_processes(services)
        self.sup = sup
        self.nac = NotificationAlertServiceClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_addSubscription(self):

        log.info('NotificationAlertTest: test_addSubscription.\n')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='NAS Add Subscription request')
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIPTION_INFO_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'test'
        reqMsg.message_parameters_reference.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscription_type = reqMsg.message_parameters_reference.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.email_alerts_filter = reqMsg.message_parameters_reference.AlertsFilter.UPDATES

        log.info("test_addSubscription: call the service")
        # try to register this user for the first time
        reply = yield self.nac.addSubscription(reqMsg)

        log.info('addSubscription returned:\n'+str(reply))

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')

        log.info('addSubscription complete')


    @defer.inlineCallbacks
    def test_removeSubscription(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # Use the message client to create a message object
        log.debug('test_removeSubscription! instantiating FindResourcesMsg.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='NAS Remove Subscription request')
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIPTION_INFO_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'test'
        reqMsg.message_parameters_reference.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscription_type = reqMsg.message_parameters_reference.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.email_alerts_filter = reqMsg.message_parameters_reference.AlertsFilter.UPDATES

        log.info('Calling removeSubscription!!...')
        reply = yield self.nac.removeSubscription(reqMsg)
        log.info('removeSubscription returned:\n'+str(reply))

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('rResponse is not an AIS_RESPONSE_MSG_TYPE GPB')

        log.info('test_removeSubscription complete')


    @defer.inlineCallbacks
    def test_getSubscriptionList(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # Use the message client to create a message object
        log.debug('test_getSubscriptionList! instantiating FindResourcesMsg.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='NAS Get Subscription List request')
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_SUBSCRIPTION_LIST_REQ_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'test'


        log.info('Calling getSubscriptionList!!...')
        reply = yield self.nac.getSubscriptionList(reqMsg)
        log.info('getSubscriptionList returned:\n'+str(reply))

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('Response is not an AIS_RESPONSE_MSG_TYPE GPB')

        log.info('test_getSubscriptionList complete')        
