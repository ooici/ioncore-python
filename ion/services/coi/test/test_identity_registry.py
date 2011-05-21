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
from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS, COMMIT_CACHE
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG, ION_AIS_RESOURCES_CFG

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
   optional string name=4;
   optional string institution=5;
   optional string email=6;
   optional string authenticating_organization=7;
   repeated net.ooici.services.coi.identity.NameValuePairType profile=8;
   optional string life_cycle_state=9;
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

        services = [
            {'name':'index_store_service','module':'ion.core.data.index_store_service','class':'IndexStoreService',
                'spawnargs':{'indices':COMMIT_INDEXED_COLUMNS}},
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True, ION_AIS_RESOURCES_CFG:True},
                          COMMIT_CACHE:'ion.core.data.store.IndexStore'}},
            {'name':'association_service', 'module':'ion.services.dm.inventory.association_service', 'class':'AssociationService'},
            {'name':'dataset_controller', 'module':'ion.services.dm.inventory.dataset_controller', 'class':'DatasetControllerClient'},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}
        ]

        sup = yield self._spawn_processes(services)

        self.irc = IdentityRegistryClient(proc=sup)
        self.mc = MessageClient(proc=self.test_sup)
        
        # initialize the user1; should not be already registered
        self.user1_subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"

        self.user1_certificate =  """-----BEGIN CERTIFICATE-----
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

        self.user1_rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
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

        # initialize the user2; should be already registered
        self.user2_subject = '/DC=org/DC=cilogon/C=US/O=Google/CN=OOI-CI OOI A539'
        self.user2_ooi_id = 'A7B44115-34BC-4553-B51E-1D87617F12E0'
        
        self.user2_certificate =  """-----BEGIN CERTIFICATE-----
MIIEVDCCAzygAwIBAgICB9QwDQYJKoZIhvcNAQELBQAwazETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRww
GgYDVQQDExNDSUxvZ29uIE9wZW5JRCBDQSAxMB4XDTExMDUyMTIxMDgwNVoXDTExMDUyMjA5MTMw
NVowZjETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMQ8wDQYDVQQKEwZHb29nbGUxGDAWBgNVBAMTD09PSS1DSSBPT0kgQTUzOTCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAJkUbOkz0SR3Xm+3uRDOrMgtm3fUkXobozVp6z12nElf
+tlQsyYxoneLjwjz97GGD5Iz+G12Hz47wqH+wKyKAsS42SaKwWFBf+IG/IjGkKNjNGk8TjmMy056
G0JyJe8V4FW3bLSvMPloaxA1HA/B1p0X83TYw2DwEFplcl5vS2dllXPlTFiENFvI6Xwo28H+AnbI
CWpb4Rek8HloyJ6M/U+bZI2rafvVWeR9E9OA6gq7s3Karn35N3NxAKmFXlYcQ6Atvm3dxod/3SDe
qENcXQDkeh+nAn31ocKbaB66UhlagMJd09Ue5hqqKZY/1epWpRL3EisEZKPW1HfRSHOm/bcCAwEA
AaOCAQUwggEBMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoGCCsGAQUF
BwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAwMwbAYDVR0fBGUwYzAvoC2gK4YpaHR0cDovL2Ny
bC5jaWxvZ29uLm9yZy9jaWxvZ29uLW9wZW5pZC5jcmwwMKAuoCyGKmh0dHA6Ly9jcmwuZG9lZ3Jp
ZHMub3JnL2NpbG9nb24tb3BlbmlkLmNybDBEBgNVHREEPTA7gRFteW9vaWNpQGdtYWlsLmNvbYYm
dXJuOnB1YmxpY2lkOklETitjaWxvZ29uLm9yZyt1c2VyK0E1MzkwDQYJKoZIhvcNAQELBQADggEB
AJ0gaeIGetkax4XNNdl5BzQYfn2gLyGBYNZCkMerbmFKiTnAnb9YNhXMWd180OTKLP/IuAjArYoz
XBDMdF5tAX8y6OIiPUxLaUoq1/nzpeXmNNbud1DKPZCn+h34n6Uk8fplCjq6bYWSsq2paA4B2/3a
paa9AI3MYPrBmBpIpW12eatLLQzJlUxUsq4znRuzSNZqjLPSXTvXpNgU5dkRx4+vXQGZGTI5xmP2
VV+kCDiccAgsGHJg2DMkudly3p1X9Y31CxxYT+t6tBG1ayhWEzsctFxCQbkryQn8WM7JDIsq/WdE
du0f+BbPVxphZTtoiQK+j6bxB4UYluKIgx7xnOo=
-----END CERTIFICATE-----"""

        self.user2_rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAmRRs6TPRJHdeb7e5EM6syC2bd9SRehujNWnrPXacSV/62VCzJjGid4uPCPP3
sYYPkjP4bXYfPjvCof7ArIoCxLjZJorBYUF/4gb8iMaQo2M0aTxOOYzLTnobQnIl7xXgVbdstK8w
+WhrEDUcD8HWnRfzdNjDYPAQWmVyXm9LZ2WVc+VMWIQ0W8jpfCjbwf4CdsgJalvhF6TweWjInoz9
T5tkjatp+9VZ5H0T04DqCruzcpquffk3c3EAqYVeVhxDoC2+bd3Gh3/dIN6oQ1xdAOR6H6cCffWh
wptoHrpSGVqAwl3T1R7mGqoplj/V6lalEvcSKwRko9bUd9FIc6b9twIDAQABAoIBAEUo9j+x+nZ4
O8FLhyAxz9wsxsWv0v4RCH60WOSO9vMrmuCd1iKWYCmUcs3/s1OQFu7d7go+SMVMKJYZy6DoRXHt
daY1IEM5XXaX43ZEB8rZoi89YLYdhyjwf+pYOg03m//9++3yDLVR2LUc2Y3A7J5S2NpcqIDeVPUS
SkaiD7Ypesk3300yIhL1F6TJPjjad9uIw4LdQLxzQZ6GAqVZNsO2i4+twUHTpZJG1wZmBwTmb52s
j8NZoxk7HONN9/o+mBkTGaGKw2xCgUpitXeiafrfaLDJBPQk8Wc1Vx9AxCDMXow59jBswDFD7WQV
e0h1SXIzT+0EUUVTmPLQjZ32rKECgYEA0y393ExPM1cThMPEGUqTCPZxcYXXZ80YslZfRahH1dam
AzvL6QlK9ZdNbkoWXsJnd8bDWHYrXTRqPn+bvGzBTkFLu6iOQaa8M/J0rYIwbBp3JA2PnwkjasQ3
G3ce3OkiCK9nQ7rDrxWMX5fs/GhzQ+rToN0RX8r1EYop6ug15PECgYEAuZGysUXNliuLiGl/6HcD
oZjjOG9miTyt/wXXvl2eAiO9nGefq7JsK6dIYVoqTO/Mar0+erLy2E+Fa+7wekj1QtMVafkE4NY7
7BgDHEWwsgGooDD6OkzD2VsjzlBesLPi8sGFmVwIMMTOZUafbJx8sQqzaj8aGugBQaqx1P5e7ScC
gYBGP9Fn/DaIhJnom1rbcvRQkfKQ6g4K6K4jfRn6SQ2EdAALqVOetMmrwuYuHxUr9o2GyaboAX9R
ZQNGwRpkZuUzDAOObHbOHhITUb9AjMNg4rjpVF2HcPnIJXeTel/Y6vC4ZOj8Hd/EmW11y0s5d+GI
IVC+/WsvK4u0hvqEuzRacQKBgQCbmD3ThCrgenyRkZwtJ/WEfrQussGv2pAuIBEIzmhZdOxcg0qP
ZZhrdeUrs7V6MyscaLdFnFwg4XSGzp8WeawkLudqpuDfQOKXkH6zKwAAEYH5Z3e4gHtK+a9pI1xy
HzLwxzElKNS5R5ujsXalVAT9UXKkaGqUGupKzDw10l93ywKBgQC8mpah+OLEbgyKkjjBYbtaY7z3
ofpx7N3GhnSPeXDhCKGp4V/bfbfCNKKpF2JjUXViE9nVpUPDRf/BN8wKZkCa7Fc/fbuZPs2bUbPW
W2hQTg2oeEpFV1QBHjlHl+l5XAvMpaD6mFhiq/vU4UtudMLxzqRl5RXx03X6JOv1f4LKSw==
-----END RSA PRIVATE KEY-----"""


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    #@itv(CONF)
    @defer.inlineCallbacks
    def test_identity_registry(self):
       
        # Test for user2 being found when calling get_user.
        log.info("testing for user2 being found when calling get_user")
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
        OoiIdRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR get_user request')
        OoiIdRequest.configuration = IdentityRequest.CreateObject(USER_OOIID_TYPE)
        OoiIdRequest.configuration.ooi_id = self.user2_ooi_id
        try:
            result = yield self.irc.get_user(OoiIdRequest)
        except ReceivedApplicationError, ex:
            self.fail("get_user failed to find a registered user")

        # Test that subject is correct
        log.info("testing user2 subject is correct")
        self.assertEqual(result.resource_reference.subject, self.user2_subject)
      
        # build GPB for test case user2
        IdentityRequest.configuration = IdentityRequest.CreateObject(IDENTITY_TYPE)
        IdentityRequest.configuration.certificate = self.user2_certificate
        IdentityRequest.configuration.rsa_private_key = self.user2_rsa_private_key

        # test that user2 is found when calling authenticate_user
        log.info("testing for user2 being found when calling authenticate_user")
        try:
            ooi_id1 = yield self.irc.authenticate_user(IdentityRequest)
        except ReceivedApplicationError, ex:
            self.fail("authenticate_user failed to find a registered user")
        
        # Test for user not found when calling get_user with user1.
        log.info("testing for user1 not found when calling get_user")
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
        OoiIdRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR get_user request')
        OoiIdRequest.configuration = IdentityRequest.CreateObject(USER_OOIID_TYPE)
        OoiIdRequest.configuration.ooi_id = "bogus-ooi_id"
        try:
            result = yield self.irc.get_user(OoiIdRequest)
            self.fail("get_user found an unregistered user")
        except ReceivedApplicationError, ex:
            self.assertEqual(ex.msg_content.MessageResponseCode, IdentityRequest.ResponseCodes.NOT_FOUND)

        # build GPB for test case user1
        IdentityRequest.configuration = IdentityRequest.CreateObject(IDENTITY_TYPE)
        IdentityRequest.configuration.certificate = self.user1_certificate
        IdentityRequest.configuration.rsa_private_key = self.user1_rsa_private_key

        # test that user1 is not found when calling authenticate_user
        log.info("testing for user1 not found when calling authenticate_user")
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
        except ReceivedApplicationError, ex:
            self.fail("register_user failed")
        
        # test that user1 is found when calling authenticate_user
        log.info("testing authentication")
        try:
            Response = yield self.irc.authenticate_user(IdentityRequest)
            log.debug('OOI_ID2 = ' + Response.resource_reference.ooi_id)
            self.assertEqual(ooi_id1, Response.resource_reference.ooi_id)
        except ReceivedApplicationError, ex:
            self.fail("Authenticate_user failed to find a registered user")
        
        # test that user1 is found when calling get_ooiid_for_user
        log.info("testing get_ooiid_for_user")
        try:
            IdentityRequest.configuration.subject = self.user1_subject
            Response = yield self.irc.get_ooiid_for_user(IdentityRequest)
            log.debug('OOI_ID2 = ' + Response.resource_reference.ooi_id)
            self.assertEqual(ooi_id1, Response.resource_reference.ooi_id)
        except ReceivedApplicationError, ex:
            self.fail("get_ooiid_for_user failed to find a registered user")
        
        # load the user back
        log.info("testing get_user")
        OoiIdRequest.configuration.ooi_id = ooi_id1
        try:
            user1 = yield self.irc.get_user(OoiIdRequest)
        except ReceivedApplicationError, ex:
            self.fail("get_user failed to find a registered user")
             
        # Test that we got a IR GPB back
        log.info("testing that GPB returned")
        self.assertNotEqual(user1, None)  

        # Test that subject is correct
        log.info("testing user1 subject is correct")
        self.assertEqual(user1.resource_reference.subject, self.user1_subject)
      
        # set the user's name, institution and email
        log.info('setting name, institution and email')
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR update user name, institution and email request')
        IdentityRequest.configuration = IdentityRequest.CreateObject(IDENTITY_TYPE)
        IdentityRequest.configuration.name = "someone"
        IdentityRequest.configuration.institution = "someplace"
        IdentityRequest.configuration.email = "someone@someplace.somedomain"
        IdentityRequest.configuration.subject = user1.resource_reference.subject
        try:
            response = yield self.irc.update_user_profile(IdentityRequest)
        except ReceivedApplicationError, ex:
            self.fail("update_user_profile failed for user %s"%IdentityRequest.configuration.subject)
        user2 = yield self.irc.get_user(OoiIdRequest)
        self.assertEqual(user2.resource_reference.name, "someone")
        self.assertEqual(user2.resource_reference.institution, "someplace")
        self.assertEqual(user2.resource_reference.email, "someone@someplace.somedomain")
        
        # Test that update works 
        log.info("testing update")
        IdentityRequest.configuration.institution = "someplace-else"
        IdentityRequest.configuration.email = "someone-else@someplace-else.some-other-domain"
        IdentityRequest.configuration.subject = user1.resource_reference.subject
        try:
            response = yield self.irc.update_user_profile(IdentityRequest)
        except ReceivedApplicationError, ex:
            self.fail("update_user_profile failed for user %s"%IdentityRequest.configuration.subject)
        user2 = yield self.irc.get_user(OoiIdRequest)
        self.assertEqual(user2.resource_reference.institution, "someplace-else")
        self.assertEqual(user2.resource_reference.email, "someone-else@someplace-else.some-other-domain")
       
        # set the user's profile
        log.info('setting profile')
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR update user profile request')
        IdentityRequest.configuration = IdentityRequest.CreateObject(IDENTITY_TYPE)
        IdentityRequest.configuration.subject = user1.resource_reference.subject
        IdentityRequest.configuration.profile.add()
        IdentityRequest.configuration.profile[0].name = "profile item 1 name"
        IdentityRequest.configuration.profile[0].value = "profile item 1 value"
        IdentityRequest.configuration.profile.add()
        IdentityRequest.configuration.profile[1].name = "profile item 2 name"
        IdentityRequest.configuration.profile[1].value = "profile item 2 value"
        log.info('IR.C = '+str(IdentityRequest.configuration))
        try:
            response = yield self.irc.update_user_profile(IdentityRequest)
        except ReceivedApplicationError, ex:
            self.fail("update_user_profile failed for user %s"%IdentityRequest.configuration.subject)
        user2 = yield self.irc.get_user(OoiIdRequest)
        log.info('user2 = '+str(user2.resource_reference.profile))
        self.assertEqual(user2.resource_reference.profile.__len__(), 2)
        self.assertEqual(user2.resource_reference.profile[0].name, "profile item 1 name")
        self.assertEqual(user2.resource_reference.profile[0].value, "profile item 1 value")
        self.assertEqual(user2.resource_reference.profile[1].name, "profile item 2 name")
        self.assertEqual(user2.resource_reference.profile[1].value, "profile item 2 value")

        # reset the user's profile to something else
        log.info('resetting profile')
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR update user profile request')
        IdentityRequest.configuration = IdentityRequest.CreateObject(IDENTITY_TYPE)
        IdentityRequest.configuration.subject = user1.resource_reference.subject
        IdentityRequest.configuration.profile.add()
        IdentityRequest.configuration.profile[0].name = "profile item 3 name"
        IdentityRequest.configuration.profile[0].value = "profile item 3 value"
        log.info('IR.C = '+str(IdentityRequest.configuration))
        try:
            response = yield self.irc.update_user_profile(IdentityRequest)
        except ReceivedApplicationError, ex:
            self.fail("update_user_profile failed for user %s"%IdentityRequest.configuration.subject)
        user2 = yield self.irc.get_user(OoiIdRequest)
        log.info('user2 = '+str(user2.resource_reference.profile))
        self.assertEqual(user2.resource_reference.profile.__len__(), 1)
        self.assertEqual(user2.resource_reference.profile[0].name, "profile item 3 name")
        self.assertEqual(user2.resource_reference.profile[0].value, "profile item 3 value") 
