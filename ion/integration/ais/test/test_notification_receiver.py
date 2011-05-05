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
        self.user_id = 0;

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_publish_recieve(self):

        log.info('NotificationReceiverTest:test_publish_receive begin')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        yield self.createUser()

        # Add a subscription for this user to this data resource
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIBE_DATA_RESOURCE_REQ_TYPE)
        reqMsg.message_parameters_reference.subscriptionInfo.user_ooi_id = self.user_id
        reqMsg.message_parameters_reference.subscriptionInfo.data_src_id = 'dataresrc123'
        reqMsg.message_parameters_reference.subscriptionInfo.subscription_type = reqMsg.message_parameters_reference.subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER
        reqMsg.message_parameters_reference.subscriptionInfo.email_alerts_filter = reqMsg.message_parameters_reference.subscriptionInfo.AlertsFilter.UPDATESANDDATASOURCEOFFLINE
        reqMsg.message_parameters_reference.subscriptionInfo.date_registered = IonTime().time_ms

        reqMsg.message_parameters_reference.datasetMetadata.user_ooi_id = self.user_id
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
                                           url="TODO")

        yield pubSourceOffline.create_and_publish_event(origin="magnet_topic",
                                           datasource_id="dataresrc123",
                                           explanation="explanation")

        yield pu.asleep(3.0)



    @defer.inlineCallbacks
    def createUser(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS RegisterUser request')
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_REQUEST_TYPE)

        # fill in the certificate and key
        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg)
        log.debug('registerUser returned:\n'+str(reply))
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != REGISTER_USER_RESPONSE_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        if reply.message_parameters_reference[0].user_is_early_adopter != True:
            self.fail("response does not indicate user is an early adopter")
        self.user_id = reply.message_parameters_reference[0].ooi_id
        log.info("NotificationAlertTest:createUser id = "+str(self.user_id))


        # Give our test user an email address
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS updateUserProfile request')
        msg.message_parameters_reference = msg.CreateObject(UPDATE_USER_PROFILE_REQUEST_TYPE)
        msg.message_parameters_reference.user_ooi_id = self.user_id
        msg.message_parameters_reference.email_address = "mmmanning@ucsd.edu"
        try:
            reply = yield self.aisc.updateUserProfile(msg)
        except ReceivedApplicationError, ex:
            self.fail('updateUserProfile incorrectly raised exception for an authenticated ooi_id')
        log.debug('updateUserProfile returned:\n'+str(reply))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
            