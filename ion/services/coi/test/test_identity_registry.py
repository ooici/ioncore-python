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
        self.user2_subject = '/DC=org/DC=cilogon/C=US/O=Google/CN=test user A501'
        self.user2_ooi_id = 'A7B44115-34BC-4553-B51E-1D87617F12E0'
        
        self.user2_certificate =  """-----BEGIN CERTIFICATE-----
MIIEUzCCAzugAwIBAgICBgIwDQYJKoZIhvcNAQELBQAwazETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRww
GgYDVQQDExNDSUxvZ29uIE9wZW5JRCBDQSAxMB4XDTExMDQyMTE5MzMyMVoXDTExMDQyMjA3Mzgy
MVowZTETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMQ8wDQYDVQQKEwZHb29nbGUxFzAVBgNVBAMTDnRlc3QgdXNlciBBNTAxMIIBIjANBgkq
hkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu+SQwAWMAY/+6eZjcirp0YfhKdgM06uZmTU9DPJqcNXF
ROFCeGEkg2jzgfcK5NiT662YbQkxETWDl4XZazmbPv787XJjYnbF8XErztauE3+caWNOpob2yPDt
mk3F0I0ullSbqsxPvsYAZNEveDBFzxCeeO+GKFQnw12ZYo968RcyZW2Fep9OQ4VfpWQExSA37FA+
4KL0RfZnd8Vc1ru9tFPw86hEstzC0Lt5HuXUHhuR9xsW3E5xY7mggHOrZWMQFiUN8WPnrHSCarwI
PQDKv8pMQ2LIacU8QYzVow74WUjs7hMd3naQ2+QgRd7eRc3fRYXPPNCYlomtnt4OcXcQSwIDAQAB
o4IBBTCCAQEwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCBLAwEwYDVR0lBAwwCgYIKwYBBQUH
AwIwGAYDVR0gBBEwDzANBgsrBgEEAYKRNgEDAzBsBgNVHR8EZTBjMC+gLaArhilodHRwOi8vY3Js
LmNpbG9nb24ub3JnL2NpbG9nb24tb3BlbmlkLmNybDAwoC6gLIYqaHR0cDovL2NybC5kb2Vncmlk
cy5vcmcvY2lsb2dvbi1vcGVuaWQuY3JsMEQGA1UdEQQ9MDuBEW15b29pY2lAZ21haWwuY29thiZ1
cm46cHVibGljaWQ6SUROK2NpbG9nb24ub3JnK3VzZXIrQTUwMTANBgkqhkiG9w0BAQsFAAOCAQEA
Omon3wMV3RFzs28iqs+r1j9WxLSvQXRXtk3BMNNmrobDspb2rodiNGMeVxGD2oGSAfh1Mn/l+vDE
1333XzQ3BGkucaSSBOTll5ZBqf52w/ru/dyrJ2GvHbIrKv+QkpKuP9uB0eJYi1n7+q/23rBR5V+E
+LsnTG8BcuzpFxtlY4SKIsijHNV+5y2+hfGHiNGfAr3X8FfwjIfmqBroCRc01ix8+jMnvplLr5rp
Wkkk8zr1nuzaUjNA/8G+24UBNSgLYOUP/xH2GlPUiAP4tZX+zGsOVkYkbyc67M4TLyD3hxuLbDCU
Aw3E0TjYpPxuQ8OsJ1LdECRfHgHFfd5KtG8BgQ==
-----END CERTIFICATE-----"""

        self.user2_rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAu+SQwAWMAY/+6eZjcirp0YfhKdgM06uZmTU9DPJqcNXFROFCeGEkg2jzgfcK
5NiT662YbQkxETWDl4XZazmbPv787XJjYnbF8XErztauE3+caWNOpob2yPDtmk3F0I0ullSbqsxP
vsYAZNEveDBFzxCeeO+GKFQnw12ZYo968RcyZW2Fep9OQ4VfpWQExSA37FA+4KL0RfZnd8Vc1ru9
tFPw86hEstzC0Lt5HuXUHhuR9xsW3E5xY7mggHOrZWMQFiUN8WPnrHSCarwIPQDKv8pMQ2LIacU8
QYzVow74WUjs7hMd3naQ2+QgRd7eRc3fRYXPPNCYlomtnt4OcXcQSwIDAQABAoIBAE7JjC0I5mlt
US4RbpfcCMnU2YTrVI2ZwkGtQllgeWOxMBQvBOlniqET7DAOQGIvsu87jtQB67JUp0ZtWPsOX9vt
nm+O7L/IID6a/wyvlrUUaKkEfGF17Jvb8zYl8JH/8Y4WEmRvYe0UJ+wej3Itg8hNJrZ9cdsNVtMk
N4JNufbH0+s2t+nZPm7jLNbXfdP6CIiyTB6OIB9M3JRKed5lpFOOsTB0HNgBFGaZvmmzWpGQJ6wQ
YsEWbMiFrB4e8qutfF+itzq5cyMrMVsAJiecMfc/j1gv+77wSi3x6tqYWgLsk5jZBNm99UM/nxWp
Xl+091gN7aha9DQ1WmCpG+D6h4kCgYEA7AuKIn/m4riQ7PsuGKNIU/h8flsO+op5FUP0NBRBY8Mc
LTon/QBcZTqpkWYblkz/ME8AEuPWKsPZQrCO9sCFRBMk0L5IZQ43kr2leB43iHDhc+OsjDB0sV8M
oEWCI4BFu7wrtbmYTqJhQaHBh0lu3jWmKnaMkWIXsF2nvqDt7VcCgYEAy8brqFssASiDFJsZB1kK
AzVkM0f43/+51fzdPW6YnrxOMt3nQqzUOF1FlmvMog/fRPjcfcttdjVu12s9DljB0AaMoBRxmKcj
/mIvxPNrTBhAHeqowZ0XyCtgEl8c+8sZUi1hUmnCIDFvi9LKXbX/mnXp0aKqWD03Hnbm/o3vaC0C
gYEAmrcFl49V+o0XEP2iPSvpIIDiuL9elgFlU/byfaA5K/aa5VoVE9PEu+Uzd8YBlwZozXU6iycj
HWy5XujzC/EsaG5T1y6hrPsgmeIMLys/IwM6Awfb9RddpVSzpelpX3OYQXEZBUfc+M2eCbLIcrBD
JwrrGzIQ+Mne1Q7OADjjOokCgYABgHbOJ9XcMFM+/KGjlzlmqqcRZa9k3zqcZB+xSzZevR6Ka24/
5Iwv2iggIq1AaIOJu5fMaYpl+6DUf5rUlzzebp3stBneOSUfw9N8TRr2VZtrXQZfXuwE8qTjncXV
6TpHi8QS2mqu2A5tZmFNbYDzv3i4rc05l0HnvJKZP6yLBQKBgERpUxpX4r5Obi8PNIECZ4ucTlhT
KJpn8B+9GrIjTqs+ae0oRfbSo1Jt/SDts/c6DYaT2RZma7JVosWd2aOAw9k69zMObHlJrcHGmb3l
eCc/SSPAJvor9B8dBoTQZbaAF4js/wffMl2Qg1WuFfyRQIAhHYO1I9aibqcJmSwDKmsL
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
            print "OOI_ID = " + ooi_id1
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
            
        """
        # Test if we can set the life cycle state
        result = yield self.irc.set_identity_lcstate_retired(ooi_id1) # Wishful thinking Roger!
        try:
            user2 = yield self.irc.get_user(OoiIdRequest)
        except ReceivedApplicationError, ex:
            self.fail("get_user failed to find a registered user")
        self.assertEqual(user2.resource_reference.life_cycle_state, 'Retired') # Should be retired now
        """
