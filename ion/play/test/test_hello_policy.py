#!/usr/bin/env python

"""
@file ion/play/test/test_hello_policy.py
@test ion.play.hello_policy Example unit tests for sample code.
@author Thomas Lennan
"""

from twisted.internet import defer

from ion.play.hello_policy import HelloPolicyClient
from ion.test.iontest import IonTestCase
import ion.util.ionlog
from ion.core import ioninit
from ion.core.exception import ReceivedApplicationError

import time

from ion.core.messaging.message_client import MessageClient

from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.core.object import object_utils

# from net.ooici.play policy_protected.proto
PROTECTED_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=20037, version=1)
PROTECTED_RESOURCE_FIND_REQ_TYPE = object_utils.create_type_identifier(object_id=20038, version=1)
PROTECTED_RESOURCE_FIND_RSP_TYPE = object_utils.create_type_identifier(object_id=20039, version=1)
PROTECTED_RESOURCE_CREATE_REQ_TYPE = object_utils.create_type_identifier(object_id=20040, version=1)
PROTECTED_RESOURCE_CREATE_RSP_TYPE = object_utils.create_type_identifier(object_id=20041, version=1)
PROTECTED_RESOURCE_UPDATE_REQ_TYPE = object_utils.create_type_identifier(object_id=20042, version=1)
PROTECTED_RESOURCE_UPDATE_RSP_TYPE = object_utils.create_type_identifier(object_id=20043, version=1)
PROTECTED_RESOURCE_DELETE_REQ_TYPE = object_utils.create_type_identifier(object_id=20044, version=1)
PROTECTED_RESOURCE_DELETE_RSP_TYPE = object_utils.create_type_identifier(object_id=20045, version=1)

IDENTITY_TYPE = object_utils.create_type_identifier(object_id=1401, version=1)
RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
RESOURCE_CFG_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class HelloPolicyTest(IonTestCase):
    """
    Testing example hello policy.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name':'association_service', 'module':'ion.services.dm.inventory.association_service', 'class':'AssociationService'},
            {'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'},
            {'name':'hello_policy','module':'ion.play.hello_policy','class':'HelloPolicy'}
        ]

        sup = yield self._spawn_processes(services)
        self.hc = HelloPolicyClient(proc=sup)
        self.irc = IdentityRegistryClient(proc=sup)
        self.mc = MessageClient(proc = self.test_sup)

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

        self.user_rsa_private_key =  """-----BEGIN RSA PRIVATE KEY-----
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

        self.admin_certificate =  """-----BEGIN CERTIFICATE-----
MIIEVDCCAzygAwIBAgICCQ4wDQYJKoZIhvcNAQELBQAwazETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRww
GgYDVQQDExNDSUxvZ29uIE9wZW5JRCBDQSAxMB4XDTExMDYwMzIxMDIxOFoXDTExMDYwNDA5MDcx
OFowZjETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMQ8wDQYDVQQKEwZHb29nbGUxGDAWBgNVBAMTD09PSS1DSSBPT0kgQTU1MjCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMIbdvzufLyoedYoWaKW8OISLcC8GfvpvhnUmrM9prEI
NHYwSfXuVlqVGHXtRUfPJj0Its+TQf7myOH5gsApqwX2MqP5QcJyO2aNWRNkTmK3XPC7gWI0Hcd5
qgwzzK3Sn6UKRjmoEcjL2vm9NaNIg8TMkj04lAG3Re59+v5uLq+cltced2QKKpxdU8EWtGMQozAu
AYaJM1avcX51ea122z49LrNCJ+2dFcpklYF61C6/A9guKkXGhk0KM+n8JU1pyKlpvmI/p8wVbgs5
GnmCaUdsyUnblXAKP3pioC1LJMRzm15YP6GLGyo8lRQviIR9efKLXoVS3PisC7eoCcyUOTcCAwEA
AaOCAQUwggEBMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoGCCsGAQUF
BwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAwMwbAYDVR0fBGUwYzAvoC2gK4YpaHR0cDovL2Ny
bC5jaWxvZ29uLm9yZy9jaWxvZ29uLW9wZW5pZC5jcmwwMKAuoCyGKmh0dHA6Ly9jcmwuZG9lZ3Jp
ZHMub3JnL2NpbG9nb24tb3BlbmlkLmNybDBEBgNVHREEPTA7gRFteW9vaWNpQGdtYWlsLmNvbYYm
dXJuOnB1YmxpY2lkOklETitjaWxvZ29uLm9yZyt1c2VyK0E1NTIwDQYJKoZIhvcNAQELBQADggEB
AAW2n6oHSRBK3hoO/7628SLh0WCesmISKzqZRm1K6EuYiLpLsgfLOZWqu27UmuxlrBNDYNs3lgL/
8VaDVo9sJMowrdWhBawALuEHrIYkX6S1HsgvcRW9n23zb1AyjwbCZlKK8QH4Moh6uByO+pOSZdbV
Lz2dw6nIoKz702VMiElLXeE1pDJIeCr5W1FJAZpi9SEWIzdjtHGojSpUx7CNupCOOTIH8R1cHbO0
mBDnP20LUI+JjtN1Va0bAHc2W8UZSsW8g4QvTBJ7XvsOGV+7XhFmxZmEhGaFDtPyCMW6E34EuRJS
9l8al9sP+u2brS6fQ5qoc5xyZVVcffYPdBFT8gY=
-----END CERTIFICATE-----"""

        self.admin_rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAwht2/O58vKh51ihZopbw4hItwLwZ++m+GdSasz2msQg0djBJ9e5WWpUYde1F
R88mPQi2z5NB/ubI4fmCwCmrBfYyo/lBwnI7Zo1ZE2ROYrdc8LuBYjQdx3mqDDPMrdKfpQpGOagR
yMva+b01o0iDxMySPTiUAbdF7n36/m4ur5yW1x53ZAoqnF1TwRa0YxCjMC4BhokzVq9xfnV5rXbb
Pj0us0In7Z0VymSVgXrULr8D2C4qRcaGTQoz6fwlTWnIqWm+Yj+nzBVuCzkaeYJpR2zJSduVcAo/
emKgLUskxHObXlg/oYsbKjyVFC+IhH158otehVLc+KwLt6gJzJQ5NwIDAQABAoIBAQCoDild4YmD
uYYK4dKBT5fs03pjZThF/+DD8muiBh2dJpJtRW+zio+fS3jrGOujuXjM3Q+R9lfsPpnr9B+9ChZ1
SewcRcEmfcpqBrT5ch3foAvKrTze7mpd+zs751ktoa7wsE2Ou7HyHHVRRfz7itvy9n8inCqgtbHJ
Q6+cu36WMUXhDlfa9hq73DN2nmKZjqaRg0rIIfyLa4fvMFWz5AtHR8FOwk79YvzOAE70MXuca0en
NmqXD/OaZ4MNXTMdPt0f2hlYOO+/rPv8DZpfi+joB9NQ+ZZqcb7nQ56yOcJx+yPjQ+8yRmheil4g
BVRr83Z41ZsCpuHpnP6FdwZNqobpAoGBAONEBtYpyktY6bbc1Z9pAy00kiGyoNFCzvtvhL3JvPZo
nC+N0aRMnlWXxxPtvyqyJIJvqK1KbWcd4yD200xhLpC5r/y3HpXJ19V+mAQ0cEsuZamw9K1fB+EY
aSlp9Foz/5cZCiX31F8yi0js+IuP1xzAv7oqup9CFry/6wp7jBm7AoGBANqmMH+0OpFVIiSjBjre
/E+sSxcqrajv4JvDns96fJjGOJ/LBY0eUUhwPY4wim0rfNcu3Hmotp/X8w0+OX0svXhu0MP0WCCJ
y/S8wNIQuXN25mqVRmU+hLFii4t7SgdxM8r1/oQKH0lAqE123zAYR417cdBurCBQA5aKKyEyTZi1
AoGAWesucUnzmkBBqHJTq1DXSumD8AVHD8TJND55XMYXF79oHICWM9WEyATXZZEpk/EL9PfM21OZ
WbU/imleTNgennB5qxmg5k8IMJZ3+yHsVDK1UqCLDpWM/oi0AwjC/3WXaOclVsRpqIjNBzuLU1zE
FcJFmZkSYbS6Xk/o5Srg0cUCgYBVRLZpNwoYH1E/ZGxLjSZsk8587GHpHhND65gFZcktczAl8PDr
RcWBMHRw/TEevfTjnhzRPSBrWbYplfipfkctrlmv8ZxkpBhsCyhPQ8Ju6xGUwz4+wZDR9JJjBOOr
31PJdQGa0K++y35XJ2KGyREuddO+60opF8subBfBzHJCeQKBgA0SkCgGHFILi80EG4FHZCdb3+CR
w/0z56l5aPSP52xpWjzPyywv+4ku+LXEyWF3qj4xJww8SVBP5nmTsYEJwu26g97ZWprehJzOOhWu
11HQQLNLNPYu68sggMAjjdguSl7W2cEJskqTWs8Gsjug0HQw/I3I9MTJKa71rsYBNdhL
-----END RSA PRIVATE KEY-----"""

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_CRUD_resource_requests(self):
        # Preliminary test setup to get OOI ID of user with DATA_PROVIDER role
        # and user with ADMIN role.
        identity_request1 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request1')
        identity_request1.configuration = identity_request1.CreateObject(IDENTITY_TYPE)
        identity_request1.configuration.certificate = self.user_certificate
        identity_request1.configuration.rsa_private_key = self.user_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response1 = yield self.irc.register_user(identity_request1)
        data_provider_ooi_id = identity_response1.resource_reference.ooi_id

        identity_request2 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request2')
        identity_request2.configuration = identity_request2.CreateObject(IDENTITY_TYPE)
        identity_request2.configuration.certificate = self.admin_certificate
        identity_request2.configuration.rsa_private_key = self.admin_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response2 = yield self.irc.register_user(identity_request2)
        admin_ooi_id = identity_response2.resource_reference.ooi_id

        create_request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='hello_create_resource request')
        create_request.configuration = create_request.CreateObject(PROTECTED_RESOURCE_CREATE_REQ_TYPE)
        create_request.configuration.name = 'SIO Buoy 1'
        create_request.configuration.description = 'SIO sensor 1'
        
        # Should fail for ANONYMOUS
        sent = False
        try:
            result = yield self.hc.hello_create_resource(create_request)
            print 'After create request anonymous'
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)

        # Retry request with user id known to have DATA_PROVIDER role
        result = yield self.hc.hello_create_resource(create_request, data_provider_ooi_id)
        self.assertFalse(result.configuration.resource_id == '')

        # Perform find operation, which is allowed to all user roles.        
        resource_uuid = result.configuration.resource_id

        find_request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='hello_find_resource request')
        find_request.configuration = create_request.CreateObject(PROTECTED_RESOURCE_FIND_REQ_TYPE)
        find_request.configuration.resource_id = resource_uuid

        # Should succeed with ANONYMOUS role.
        result = yield self.hc.hello_find_resource(find_request)
        
        # Perform update operation, which requires the ADMIN role.
        update_request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='hello_update_resource request')
        update_request.configuration = create_request.CreateObject(PROTECTED_RESOURCE_UPDATE_REQ_TYPE)
        update_request.configuration.resource_id = resource_uuid
        update_request.configuration.name = 'SIO Buoy La Jolla'
        update_request.configuration.description = 'SIO sensor, La Jolla, CA'

        # Should fail for ANONYMOUS
        sent = False
        try:
            result = yield self.hc.hello_update_resource(update_request)
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)

        # Retry request with user id with 'super user' role (ie. ADMIN)
        result = yield self.hc.hello_update_resource(update_request, admin_ooi_id)
        self.assertTrue(result.configuration.status == 'OK')
        
        # Perform delete operation, which requires the OWNER role.
        delete_request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='hello_delete_resource request')
        delete_request.configuration = create_request.CreateObject(PROTECTED_RESOURCE_DELETE_REQ_TYPE)
        delete_request.configuration.resource_ids.append(resource_uuid)

        # Should fail for ANONYMOUS
        sent = False
        try:
            result = yield self.hc.hello_delete_resource(delete_request)
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)

        # Retry request with user id that created resource (ie. OWNER)
        result = yield self.hc.hello_delete_resource(delete_request, data_provider_ooi_id)
        self.assertTrue(result.configuration.status == 'OK')

    @defer.inlineCallbacks
    def test_hello_anonymous_request_anonymous(self):
        # Default send behavior today.
        #
        # Call service via service client.  Service client will use standard
        # RPC send. This will be an ANONYMOUS request. Service operation
        # in res/config/ionpolicydb.cfg has been marked as allowing
        # anonymous access.
        #
        # HelloPolicyClient ---> rpc_send() ---> HelloPolicy 
        result = yield self.hc.hello_request('hello_anonymous_request')
        self.assertEqual(result, "{'value': 'ANONYMOUS'}")

    @defer.inlineCallbacks
    def test_hello_anonymous_request_userid(self):
        # New behavior illustrating rpc_send_protected method and policy enforcement.
        # Service operation in res/config/ionpolicydb.cfg has been marked as
        # allowing anonymous access.
        #
        # Call service via service client.  Service client will use protected
        # RPC send, passing user id 'MYUSER'.
        # HelloPolicyClient ---> rpc_send_protected('MYUSER') ---> HelloPolicy
        current_time = int(time.time())
        expiry = str(current_time + 30)

        result = yield self.hc.hello_request('hello_anonymous_request', 'MYUSER', expiry)
        self.assertEqual(result, "{'value': 'MYUSER'}")

    @defer.inlineCallbacks
    def test_hello_authenticated_request_fail_not_auth(self):
        # Default send behavior today.
        #
        # Call service via service client.  Service client will use standard
        # RPC send. This will be an ANONYMOUS request. However, this test
        # will fail because Service operation in res/config/ionpolicydb.cfg
        # has been marked as requiring authenticated access.
        #
        # HelloPolicyClient ---> rpc_send() ---> HelloPolicy 
        sent = False
        try:
            result = yield self.hc.hello_request('hello_authenticated_request')
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)

    @defer.inlineCallbacks
    def test_hello_authenticated_request(self):
        # New behavior illustrating rpc_send_protected method and policy enforcement.
        # Service operation in res/config/ionpolicydb.cfg has been marked as
        # requiring authenticated access.
        #
        # Call service via service client.  Service client will use protected
        # RPC send, passing user id 'MYUSER'.
        # HelloPolicyClient ---> rpc_send_protected('MYUSER') ---> HelloPolicy
        current_time = int(time.time())
        expiry = str(current_time + 30)
        
        result = yield self.hc.hello_request('hello_authenticated_request', 'MYUSER', expiry)
        self.assertEqual(result, "{'value': 'MYUSER'}")

    @defer.inlineCallbacks
    def test_hello_authenticated_request_fail_expiry(self):
        # New behavior illustrating rpc_send_protected method and policy enforcement.
        # Service operation in res/config/ionpolicydb.cfg has been marked as
        # requiring authenticated access.  However, expiry will be exceeded.
        #
        # Call service via service client.  Service client will use protected
        # RPC send, passing user id 'MYUSER'.
        # HelloPolicyClient ---> rpc_send_protected('MYUSER') ---> HelloPolicy
        expiry = '12345'
        
        sent = False
        try:
            result = yield self.hc.hello_request('hello_authenticated_request', 'MYUSER', expiry)
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)

