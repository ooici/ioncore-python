#!/usr/bin/env python

"""
@file ion/integration/test_notification_alert.py
@test ion.integration.notification_alert_service
@author Maurice Manning
"""
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.integration.ais.notification_alert_service import NotificationAlertServiceClient
from ion.integration.ais.app_integration_service import AppIntegrationServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.test.iontest import IonTestCase
from ion.util.iontime import IonTime

from ion.services.coi.datastore_bootstrap.ion_preload_config import MYOOICI_USER_ID

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       GET_SUBSCRIPTION_LIST_REQ_TYPE, \
                                                       SUBSCRIBE_DATA_RESOURCE_REQ_TYPE



class NotificationAlertTest(IonTestCase):
    """
    Testing Notification Alert Service.
    """
    services = [
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
                    'spawnargs':{'datastore_service':'datastore'}
            },

            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },

            {
                'name':'identity_registry',
                'module':'ion.services.coi.identity_registry',
                'class':'IdentityRegistryService'
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

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

       
        sup = yield self._spawn_processes(self.services)
        self.sup = sup
        self.nac = NotificationAlertServiceClient(proc=sup)
        self.aisc = AppIntegrationServiceClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    def test_instantiate(self):
        pass
    
    @defer.inlineCallbacks
    def test_addSubscription(self):

        log.info('NotificationAlertTest:NotificationAlertTest: test_addSubscription.\n')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0
        
        reply = yield self.nac.addSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # Call the Alert service to retrieve the list of subscriptions for a user
        log.info('NotificationAlertTest: test_getSubscriptionList call get subscription list service.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_SUBSCRIPTION_LIST_REQ_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = MYOOICI_USER_ID 

        reply = yield self.nac.getSubscriptionList(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('Response is not an AIS_RESPONSE_MSG_TYPE GPB')

        numResReturned = len(reply.message_parameters_reference[0].subscriptionListResults)
        if numResReturned != 1:
            self.fail('NotificationAlertTest: test_addSubscription returned incorrect subscription count.')

        
    @defer.inlineCallbacks
    def test_removeSubscription(self):

        log.info('NotificationAlertTest:test_removeSubscription.\n')
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0
        
        reply = yield self.nac.addSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # Call the Alert service to remove this specific subscription based on user id and resource id
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reply = yield self.nac.removeSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('Response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # Call the Alert service to retrieve the list of subscriptions for a user
        log.info('NotificationAlertTest: test_getSubscriptionList call get subscription list service.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_SUBSCRIPTION_LIST_REQ_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = MYOOICI_USER_ID

        reply = yield self.nac.getSubscriptionList(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('Response is not an AIS_RESPONSE_MSG_TYPE GPB')

        numResReturned = len(reply.message_parameters_reference[0].subscriptionListResults)
        if numResReturned != 0:
            self.fail('NotificationAlertTest: test_removeSubscription returned incorrect subscription count.')


    @defer.inlineCallbacks
    def test_getSubscriptionList(self):

        log.info('NotificationAlertTest: test_getSubscriptionList.\n')
        # Create a message client
        mc = MessageClient(proc=self.test_sup)


        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0
        reply = yield self.nac.addSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationAlertTest: test_getSubscriptionList response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset456'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset456'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -55.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -45.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 25.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 35.0
        reply = yield self.nac.addSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationAlertTest: test_getSubscriptionList response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # Call the Alert service to retrieve the list of subscriptions for a user
        log.info('NotificationAlertTest: test_getSubscriptionList call get subscription list service.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_SUBSCRIPTION_LIST_REQ_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = MYOOICI_USER_ID

        reply = yield self.nac.getSubscriptionList(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationAlertTest: test_getSubscriptionList Response is not an AIS_RESPONSE_MSG_TYPE GPB')

        numResReturned = len(reply.message_parameters_reference[0].subscriptionListResults)
        if numResReturned != 2:
            self.fail('NotificationAlertTest: test_getSubscriptionList returned incorrect subscription count.')


    @defer.inlineCallbacks
    def test_getSubscription(self):

        log.info('NotificationAlertTest: test_getSubscription.\n')
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset123'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -50.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -40.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 20.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 30.0
        reply = yield self.nac.addSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationAlertTest: test_getSubscription response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # Add a second subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset456'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATES
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.datasetMetadata.data_resource_id = 'dataset456'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_start = '2007-01-1T00:02:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_time_coverage_end = '2007-01-1T00:03:00Z'
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min = -55.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max = -45.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min = 25.0
        reqMsg.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max = 35.0
        reply = yield self.nac.addSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationAlertTest: test_getSubscription response is not an AIS_RESPONSE_MSG_TYPE GPB')

        # Call the Alert service to retrieve the subscription for a user
        log.info('NotificationAlertTest: test_getSubscription call get subscription service.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = MYOOICI_USER_ID
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataset123'

        reply = yield self.nac.getSubscription(reqMsg)

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('NotificationAlertTest: test_getSubscription Response is not an AIS_RESPONSE_MSG_TYPE GPB')

        numResReturned = len(reply.message_parameters_reference[0].subscriptionListResults)
        if numResReturned != 1:
            self.fail('NotificationAlertTest: test_getSubscription returned incorrect subscription count.')
        log.info('getSubscription returned:\n %s'%reply.message_parameters_reference[0].subscriptionListResults[0])

