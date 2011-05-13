#!/usr/bin/env python

"""
@file ion/integration/test_notification_alert.py
@test ion.integration.notification_alert_service
@author Maurice Manning
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient
from ion.integration.ais.app_integration_service import AppIntegrationServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.test.iontest import IonTestCase
from ion.core.exception import ReceivedApplicationError
from ion.core.process.process import Process
from ion.util.iontime import IonTime

import ion.util.procutils as pu
from ion.services.dm.distribution.events import DatasetSupplementAddedEventPublisher, DatasourceUnavailableEventPublisher
from ion.services.coi.datastore_bootstrap.ion_preload_config import MYOOICI_USER_ID

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       REGISTER_USER_REQUEST_TYPE, \
                                                       REGISTER_USER_RESPONSE_TYPE, \
                                                       UPDATE_USER_PROFILE_REQUEST_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_RSP_TYPE

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
                'name':'app_integration',
                'module':'ion.integration.ais.app_integration_service',
                'class':'AppIntegrationService'
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
        self.aisc = AppIntegrationServiceClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_publish_recieve(self):

        log.info('NotificationReceiverTest:test_publish_receive begin')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)


        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataresrc123'
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATESANDDATASOURCEOFFLINE
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataresrc123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0

        # try to register this user for the first time
        reply = yield self.nac.addSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationReceiverTest: response is not an AIS_RESPONSE_MSG_TYPE GPB')

        pubSupplementAdded = DatasetSupplementAddedEventPublisher(process=self.test_sup) # all publishers/subscribers need a process associated
        yield pubSupplementAdded.initialize()
        yield pubSupplementAdded.activate()

        pubSourceOffline = DatasourceUnavailableEventPublisher(process=self.test_sup) # all publishers/subscribers need a process associated
        yield pubSourceOffline.initialize()
        yield pubSourceOffline.activate()

        # creates the event notification for us and sends it



        yield pubSupplementAdded.create_and_publish_event(origin="magnet_topic",
                                           dataset_id="dataresrc123",
                                           datasource_id="dataresrc123",
                                           title="TODO",
                                           url="TODO",
                                           start_datetime_millis = 10000,
                                           end_datetime_millis = 11000,
                                           number_of_timesteps = 7
                                     )



        yield pubSourceOffline.create_and_publish_event(origin="magnet_topic",
                                           datasource_id="dataresrc123",
                                           error_explanation="explanation")
        

        yield pu.asleep(3.0)



            