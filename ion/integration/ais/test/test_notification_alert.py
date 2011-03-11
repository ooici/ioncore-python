__author__ = 'mauricemanning'
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
from ion.core.messaging.message_client import MessageClient
from ion.test.iontest import IonTestCase

from ion.core.object import object_utils

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import REGISTER_USER_TYPE, \
                                                       OOI_ID_TYPE, \
                                                       FIND_DATA_RESOURCES_MSG_TYPE, \
                                                       SUBSCRIPTION_INFO_TYPE
                                                       


class NotificationAlertTest(IonTestCase):
    """
    Testing Notification Alert Service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'notification_alert','module':'ion.integration.ais.notification_alert_service','class':'NotificationAlertService'},
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'},
            {'name' : 'attributestore', 'module' : 'ion.services.coi.attributestore', 'class' : 'AttributeStoreService'},
        ]
        sup = yield self._spawn_processes(services)

        self.sup = sup

        self.nac = NotificationAlertServiceClient(proc=sup)
        #self.rc = ResourceClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()



    @defer.inlineCallbacks
    def test_addSubscription(self):

        log.info('NotificationAlertService: test_addSubscription.\n')

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='NAS Add Subscription request')
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIPTION_INFO_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = 'test';
        reqMsg.message_parameters_reference.queue_id = 'test';

        log.info("test_addSubscription: call the service")
        # try to register this user for the first time
        reply = yield self.nac.addSubscription(reqMsg)
        log.debug('addSubscription returned:\n'+str(reply))

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')


    @defer.inlineCallbacks
    def test_removeSubscription(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # Use the message client to create a message object
        log.debug('NotificationAlertService! instantiating FindResourcesMsg.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='NAS Remove Subscription request')
        reqMsg.message_parameters_reference = reqMsg.CreateObject(SUBSCRIPTION_INFO_TYPE)
        reqMsg.message_parameters_reference.user_ooi_id = "test";
        reqMsg.message_parameters_reference.queue_id = "test";

        log.debug('Calling removeSubscription!!...')
        reply = yield self.nac.removeSubscription(reqMsg)
        log.debug('removeSubscription returned:\n'+str(reply))

        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('rResponse is not an AIS_RESPONSE_MSG_TYPE GPB')
        