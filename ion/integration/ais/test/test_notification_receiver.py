__author__ = 'mauricemanning'
#!/usr/bin/env python

"""
@file ion/integration/test_notification_receiver.py
@test ion.integration.notification_alert_service
@author Maurice Manning
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.test.iontest import IonTestCase
from ion.core.process.process import Process

import ion.util.procutils as pu
from ion.services.dm.distribution.events import ResourceLifecycleEventPublisher


# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import SUBSCRIPTION_INFO_TYPE


class NotificationReceiverTest(IonTestCase):

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
    def test_publish_recieve(self):
        """
        """

        log.info('NotificationReceiverTest: test_publish_recieve begin')
        proc = Process()
        yield proc.spawn()

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='NAS Add Subscription request')
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIPTION_INFO_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'test'
        reqMsg.message_parameters_reference.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscription_type = reqMsg.message_parameters_reference.SubscriptionType.EMAIL
        reqMsg.message_parameters_reference.email_alerts_filter = reqMsg.message_parameters_reference.AlertsFilter.UPDATES

        log.info("NotificationReceiverTest: call the service")
        # try to register this user for the first time
        reply = yield self.nac.addSubscription(reqMsg)
        log.info('addSubscription returned:\n'+str(reply))

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationReceiverTest: response is not an AIS_RESPONSE_MSG_TYPE GPB')

        log.info('NotificationReceiverTest: test_publish_recieve publish notification')

        pub = ResourceLifecycleEventPublisher(process=self.sup) # all publishers/subscribers need a process associated
        yield pub.initialize()
        yield pub.activate()

        yield pub.create_and_publish_event(name="foo", state=ResourceLifecycleEventPublisher.State.NEW, origin="magnet_topic")


        yield pu.asleep(3.0)

        log.info('NotificationReceiverTest: test_publish_recieve completed')
