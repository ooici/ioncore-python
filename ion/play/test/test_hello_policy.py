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

        self.user_rsa_private_key =  """-----BEGIN RSA PRIVATE KEY-----
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

        self.admin_certificate =  """-----BEGIN CERTIFICATE-----
MIIEbzCCA1egAwIBAgICCCowDQYJKoZIhvcNAQELBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTEwNDI2MjI0MjU2WhcNMTEwNDI3MTA0NzU2
WjCBhTETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMSswKQYDVQQKEyJVbml2ZXJzaXR5IG9mIENhbGlmb3JuaWEtU2FuIERpZWdvMRswGQYD
VQQDExJUaG9tYXMgTGVubmFuIEE0NDYwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCi
Txfj08iQP43jNZ+1IOpHD1OXb+OZHGl5cEecy5yYLATLRJMCbL86DTXZYUGeQ5CMqPkKzFKGLGZ+
AO3suFTLg3a5GZYGkV4gIr9wUN5PCel0rQ1fTedbc5ycYzJkll35LMT9XjhHTcSq750qNMeITrOk
i7GYzJD34kf2hi2/f5UujkPFIiOghbFWQ5AozFZ7UaCKY7ZPt20sIc0UJu41i3f7f7w71u7BJu7h
DkGG5dz8IUUgwGcUmoUqirg6mV87fDP6cJPKbvbrz0r0Lw7SLpLm78lizbOXBOcIoZAJ9KJg1BGf
LuNjCZB4LoV5/yxWV58UOtxeF0M8VWeGL1nnAgMBAAGjggEBMIH+MAwGA1UdEwEB/wQCMAAwDgYD
VR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoGCCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYB
AgIwagYDVR0fBGMwYTAuoCygKoYoaHR0cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2lj
LmNybDAvoC2gK4YpaHR0cDovL2NybC5kb2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwQwYD
VR0RBDwwOoEQdGxlbm5hbkB1Y3NkLmVkdYYmdXJuOnB1YmxpY2lkOklETitjaWxvZ29uLm9yZyt1
c2VyK0E0NDYwDQYJKoZIhvcNAQELBQADggEBAKHfbciULW6AvGgGRNq+T5apHNAQn0AAyjiy7hU4
+3/M4c1Z3bMoDBTF2svR6C3ukFbVWrdHqwoqjUeLTk7IhmkOsZYEfVVYYMZpEnBDE0x9HvJVrX6D
4wMMpPF4ksF0Bv2QJAZZgrJ5zVhhDtgOymriAvMBfHRwT03V1SGdxrKx6CHVkf30oEBWjwW1eg7U
9mEOKeKJnbPOXc+YUJuku8yLJCVgfLTdvu2XwOW6Gjg3ohWmiL2/tnCMNaTO08kGLHnnAUgdiHH6
7a+pC34Ko1X++dR+ucfEcwlM1CxG6ljq2RkbAEF4YOQM91Wz/tOvdU5Sz1DEA+CNq0TpqCAWAyU=
-----END CERTIFICATE-----"""

        self.admin_rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAok8X49PIkD+N4zWftSDqRw9Tl2/jmRxpeXBHnMucmCwEy0STAmy/Og012WFB
nkOQjKj5CsxShixmfgDt7LhUy4N2uRmWBpFeICK/cFDeTwnpdK0NX03nW3OcnGMyZJZd+SzE/V44
R03Equ+dKjTHiE6zpIuxmMyQ9+JH9oYtv3+VLo5DxSIjoIWxVkOQKMxWe1GgimO2T7dtLCHNFCbu
NYt3+3+8O9buwSbu4Q5BhuXc/CFFIMBnFJqFKoq4OplfO3wz+nCTym72689K9C8O0i6S5u/JYs2z
lwTnCKGQCfSiYNQRny7jYwmQeC6Fef8sVlefFDrcXhdDPFVnhi9Z5wIDAQABAoIBAECelLhT3Qnx
A6Bt/QOz8xIPfAxSs56FvUhn91rphZLgE5iJ2K2idg+6qrG9Es7bv3UA4QX3ivN4QeXwoMkaKkhM
MrxG/3/l6D+LFgS1bYyyOYwGScijz3SNdtCcfbemVguaU2M8W9OnlqEnfL8M/FO8YbElHLPk3eKS
6GuxyAzxL/iYD3Y7xxzwYihqsfeZ60f6lABT7iVTPu2PAE67wj+07m0e7hrFr9C+8ncUcen0QsjU
o+DbSoBjZdb8SAHPgSmQfwHOG/5pxMqz2O9lPcIpWIATEVETpcdlGchz+uFC+nQf/J+dq1UmaqbJ
rE+CiSKLiKySny+0fwaw1bJcp4ECgYEA12teKXy/tvZrOJysxpwMd4cjhIqQrKDRsE+XYAhLOaob
up2ocw3xMGH/jeAcKihNIaqWXlC9CF0Pokl1pWnVR1G6BU7G6aHbhfZ10gu00YhKMurgz6PclsD0
VzNhWntDUqKf4iJp/FwksKfeVZUxTsAamz8HH9LkSlKJmd/U/3ECgYEAwOJ3aWEx+2TrCEct5jdF
/A44lpMTODdhZgrA12ytRJTxJrikblQL3Xg+dpGnyhIz+wr8ADqm6c6ZswPpZyJSzQ+dSSOMLGj1
/r+nsU/KShavaAQbrXNiYxhws85dklJ4rsQHC6jv9yEX0TPWVn/+P87xHxF9j2NoM7YcXvwU8tcC
gYEAsd2VAexe31LXdQboIZT3Njn+uZEvo1mlyU3uTvyKIDK0coF6dIUugCRqPVqt5qEgDowrW/SO
IDm2jujYmpun3hs83OUOmBlsiE/XOHcx9U/y89e7h7ZkjJUFKnriBzN/gtuD59NCb3wlTzdL/J4S
+FLlpQmFeGFI3ZUIyGRoGgECgYEAiiyxpkf+ajztMczr0JOgCuSVHSjXB9qEZ3kklC5CIXMhHb//
6xlCNrFA7eeB73wCyMAhrnhVwgBCkr8moL8x2bxpzE8ux1GZh5j8JEUogNKnbTgkK6kQvjRv6B7Z
YUy7L1c2ROAMp1iJm1ArJ2QhWsyAZuU8sU9hAkpfIJws1ZkCgYEAlB0HJywJkYgx7ndZiGC0ZtP4
MEssyRkX8I1MhfJuJLJ2Khe03DqxqaUwm1ECIS1+yVaoXlvyc+30qWG/WL7VMfTagU4w7EeZTQLc
uPZqyQ9Ntw1mrqDbGO9es7PKsFOG1mHHy76A0Aens0WRA8lGLpIr0rGgLeFIsmL2NaQiEzE=
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

