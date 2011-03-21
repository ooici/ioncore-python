#!/usr/bin/env python

"""
@file ion/services/coi/test/test_identity_registry.py
@author Roger Unwin, Bill Bollenbacher
@brief test Idenity Registry Service
"""
from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest

from ion.core.exception import ReceivedError
from ion.test.iontest import IonTestCase
from ion.services.coi.identity_registry import IdentityRegistryClient
from ion.core.exception import ReceivedApplicationError

from ion.resources import coi_resource_descriptions
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

from ion.util.itv_decorator import itv

CONF = ioninit.config(__name__)

IDENTITY_TYPE = object_utils.create_type_identifier(object_id=1401, version=1)
"""
from ion-object-definitions/net/ooici/services/coi/identity/identity_management.proto
message UserIdentity {
   enum _MessageTypeIdentifier {
       _ID = 1401;
       _VERSION = 1;
   }

   // objects in a protofile are called messages

   optional string subject=1;
   optional string certificate=2;
   optional string rsa_private_key=3;
   optional string dispatcher_queue=4;
   optional string email=5;
   optional string life_cycle_state=6;
}
"""""

USER_OOIID_TYPE = object_utils.create_type_identifier(object_id=1403, version=1)
"""
from ion-object-definitions/net/ooici/services/coi/identity/identity_management.proto
message UserOoiId {
   enum _MessageTypeIdentifier {
       _ID = 1403;
       _VERSION = 1;
   }

   // objects in a protofile are called messages

   optional string ooi_id=1;
}
"""

RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
message ResourceConfigurationRequest{
    enum _MessageTypeIdentifier {
      _ID = 10;
      _VERSION = 1;
    }
    
    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
"""

RESOURCE_CFG_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
message ResourceConfigurationResponse{
    enum _MessageTypeIdentifier {
      _ID = 12;
      _VERSION = 1;
    }
    
    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
    
    optional string result = 3;
}
"""


class IdentityRegistryClientTest(IonTestCase):
    """
    Integration testing client class of Identity Registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [{'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService', 'spawnargs':{'servicename':'datastore'}},
                    {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService', 'spawnargs':{'datastore_service':'datastore'}},
                    {'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}]

        sup = yield self._spawn_processes(services)

        self.irc = IdentityRegistryClient(proc=sup)
        self.mc = MessageClient(proc=self.test_sup)
        
        # initialize the user
        self.user_subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"

        self.user_certificate =  """-----BEGIN CERTIFICATE-----
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

        self.user_rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
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


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    #@itv(CONF)
    @defer.inlineCallbacks
    def test_identity_registry(self):

        # test that user is not yet registered
        found = yield self.irc.is_user_registered(self.user_certificate, self.user_rsa_private_key)
        self.assertEqual(found, False)
        
        # build GPB for test case user
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request')
        IdentityRequest.configuration = IdentityRequest.CreateObject(IDENTITY_TYPE)
        IdentityRequest.configuration.certificate = self.user_certificate
        IdentityRequest.configuration.rsa_private_key = self.user_rsa_private_key

        # test that user is not found
        log.info("testing for user not found")
        try:
            ooi_id1 = yield self.irc.authenticate_user(IdentityRequest)
            self.fail("Authenticate_user found an unregistered user")
        except ReceivedApplicationError, ex:
            self.assertEqual(ex.msg_content.MessageResponseCode, IdentityRequest.ResponseCodes.NOT_FOUND)
        
        # Register a user, this shouldn't fail
        log.info("registering user")
        try:
            Response = yield self.irc.register_user(IdentityRequest)
            ooi_id1 = Response.resource_reference.ooi_id
            log.debug('OOI_ID1 = ' + ooi_id1)
            print "OOI_ID = " + ooi_id1
        except ReceivedApplicationError, ex:
            self.fail("register_user failed")
        
        # Verify we can find it.
        log.info("testing for finding registered user")
        found = yield self.irc.is_user_registered(self.user_certificate, self.user_rsa_private_key)
        self.assertEqual(found, True)
        
        log.info("testing authentication")
        try:
            ooi_id2 = yield self.irc.authenticate_user(IdentityRequest)
            log.debug('OOI_ID2 = ' + ooi_id2)
            self.assertEqual(ooi_id1, ooi_id2)
        except ReceivedApplicationError, ex:
            self.fail("Authenticate_user failed to find a registered user")
        
        # load the user back
        log.info("testing get_user")
        OoiIdRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR get_user request')
        OoiIdRequest.configuration = IdentityRequest.CreateObject(USER_OOIID_TYPE)
        OoiIdRequest.configuration.ooi_id = ooi_id1
        try:
            user1 = yield self.irc.get_user(OoiIdRequest)
        except ReceivedApplicationError, ex:
            self.fail("get_user failed to find a registered user")
             
        # Test that we got a IR GPB back
        log.info("testing that GPB returned")
        self.assertNotEqual(user1, None)  

        # Test that subject is correct
        log.info("testing subject is correct")
        self.assertEqual(user1.resource_reference.subject, "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254")
      
        # Test that update work
        log.info("testing update")
        IdentityRequest.configuration.certificate = self.user_certificate + "\nA Small Change"
        IdentityRequest.configuration.subject = user1.resource_reference.subject
        try:
            response = yield self.irc.update_user(IdentityRequest)
        except ReceivedApplicationError, ex:
            self.fail("update_user failed for user %s"%IdentityRequest.configuration.subject)
        user2 = yield self.irc.get_user(OoiIdRequest)
        self.assertEqual(user2.resource_reference.certificate, self.user_certificate + "\nA Small Change")
       
        # Test if we can find the user we have stuffed in.
        user_description = coi_resource_descriptions.IdentityResource()
        user_description.subject = 'Roger'

        # Disabled until find is properly implemented
        #users1 = yield self.irc.find_users(user_description,regex=True)
        #self.assertEqual(len(users1), 1) # should only return 1 match
        #self.assertEqual("/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254 CHANGED", users1[0].subject)
             
        # Test if we can set the life cycle state
        result = yield self.irc.set_identity_lcstate_retired(ooi_id1) # Wishful thinking Roger!
        try:
            user2 = yield self.irc.get_user(OoiIdRequest)
        except ReceivedApplicationError, ex:
            self.fail("get_user failed to find a registered user")
        self.assertEqual(user2.resource_reference.life_cycle_state, 'Retired') # Should be retired now

        # Test for user not found handled properly.
        OoiIdRequest.configuration.ooi_id = "bogus-ooi_id"
        try:
            result = yield self.irc.get_user(OoiIdRequest)
            self.fail("get_user found an unregistered user")
        except ReceivedApplicationError, ex:
            self.assertEqual(ex.msg_content.MessageResponseCode, IdentityRequest.ResponseCodes.NOT_FOUND)

