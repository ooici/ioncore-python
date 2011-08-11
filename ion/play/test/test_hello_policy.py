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

from ion.core.process.cprocess import Invocation

from ion.core.intercept.policy import PolicyInterceptor

import time

from ion.core.security.authentication import Authentication

from ion.core.messaging.message_client import MessageClient

from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.core.object import object_utils

from ion.core.intercept.policy import subject_has_role, \
                                      user_has_role, \
                                      subject_has_admin_role, \
                                      user_has_admin_role, \
                                      map_ooi_id_to_subject_admin_role, \
                                      subject_has_early_adopter_role, \
                                      user_has_early_adopter_role, \
                                      map_ooi_id_to_subject_early_adopter_role, \
                                      subject_has_data_provider_role, \
                                      user_has_data_provider_role, \
                                      map_ooi_id_to_subject_data_provider_role, \
                                      subject_has_marine_operator_role, \
                                      user_has_marine_operator_role, \
                                      map_ooi_id_to_subject_marine_operator_role

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

        self.user1_rsa_private_key =  """-----BEGIN RSA PRIVATE KEY-----
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

        self.user2_certificate =  """-----BEGIN CERTIFICATE-----
MIIEXDCCA0SgAwIBAgICCtgwDQYJKoZIhvcNAQELBQAwazETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRww
GgYDVQQDExNDSUxvZ29uIE9wZW5JRCBDQSAxMB4XDTExMDYyNDE3MzkwMloXDTExMDYyNTA1NDQw
MlowaTETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMQ8wDQYDVQQKEwZHb29nbGUxGzAZBgNVBAMTElRob21hcyBMZW5uYW4gQTQ3NjCCASIw
DQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAImFRROqThZSXJYiQogJE5HosyOtXJqp3T/ylI6x
sy6yIv1BiYtm8GEgUsfIZhxy51RXdpK2Q7s7G+z+ES9dQhQX9DJ7xbZLEV3Z039rKpNsvbBxE93I
HOeT8vrzHEhDJLuDWxCssLra5USI9Z3pA9QSOsZCZu9BHS8Ur+fTQc8xPqx1fPfRHRAMHosMFtP/
giZ2cIojMkPA73huk32eC5mDlBG+QKuLPQQDwN8LWMjmGFrPrPuDRIOIQ3LZnEDckcn4jVXheYEt
YJzBb0AjJ8pi7Bc7z2TG6AJXPY44iMczoT00XZjyeKksY3VmLM74ta7KYFRVUCO4yuyBPRx29rMC
AwEAAaOCAQowggEGMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoGCCsG
AQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAwMwbAYDVR0fBGUwYzAvoC2gK4YpaHR0cDov
L2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLW9wZW5pZC5jcmwwMKAuoCyGKmh0dHA6Ly9jcmwuZG9l
Z3JpZHMub3JnL2NpbG9nb24tb3BlbmlkLmNybDBJBgNVHREEQjBAgRZ0aG9tYXNsZW5uYW5AZ21h
aWwuY29thiZ1cm46cHVibGljaWQ6SUROK2NpbG9nb24ub3JnK3VzZXIrQTQ3NjANBgkqhkiG9w0B
AQsFAAOCAQEAj8DNlQAJDNlvNqlMTajEivnRcW/Ulr+cZiiO+40Zll7MQ3b3drzppLJfVxG8HFbJ
ddnZ+JerHWKImumjYLgKas6mQ/gOlIAcWYE1FCqBBlBmPpY75QMAo9ZPEVE3QrOo8wK9D95XRm/0
WWy8zke77Q27wg1l+uBcYvOugmqzia5o+4XI3ao15lyTisTlKbpgT0cAvQXin5L5vSFIbmyED2A6
FiAW3A4pc9ub/KYP/dP8LEsnqsEook0eubP24MgE+08AZp0sDbbtmZteiGo1wHMlQHhss0MNbkdK
a8jWY22kaLQUdS2tXaRQLUYo2H65m8KvNvf+im2oczqs48xG7w==
-----END CERTIFICATE-----"""

        self.user2_rsa_private_key =  """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAiYVFE6pOFlJcliJCiAkTkeizI61cmqndP/KUjrGzLrIi/UGJi2bwYSBSx8hm
HHLnVFd2krZDuzsb7P4RL11CFBf0MnvFtksRXdnTf2sqk2y9sHET3cgc55Py+vMcSEMku4NbEKyw
utrlRIj1nekD1BI6xkJm70EdLxSv59NBzzE+rHV899EdEAweiwwW0/+CJnZwiiMyQ8DveG6TfZ4L
mYOUEb5Aq4s9BAPA3wtYyOYYWs+s+4NEg4hDctmcQNyRyfiNVeF5gS1gnMFvQCMnymLsFzvPZMbo
Alc9jjiIxzOhPTRdmPJ4qSxjdWYszvi1rspgVFVQI7jK7IE9HHb2swIDAQABAoIBABE6zEvJc50i
Vo1M348RrA0E3aTjrI2IKLtBVlGGfA+mq/GVC3mWvRk+JoD3X6vCza7ogmehRF0p67bGojqP8Z54
3dSRY1USlKtwhioZsCzmW+HGWRnZX524EKJWYT3Ag9Kmg3tUV5QhpsXubu+I6Tzhx9FdMm5ZdyGV
8vAVrNIbhmygjv03/Tr7fZSkpa/ehspoWQKO/LHY8P+r6wCNpOYNcwEsk4A9biYEgGxOuxYucy0t
E0EdA66mT1mQhQWv/yEwsh0UDPRs22g+NokkQeFYbPRU+15R1azb2qGX8XViGdQvZZT+2G+ZrTFf
+NC3rAFGj/vnXDFDidqLaMSvSTkCgYEA5RuU2aDsGq+Wgby7APAbSF1O7i+lPdgDNK8qftsDvkGs
cCZ6w0cAFi2cOmUu1rjg+f54wU9jFYh3BOOFB1GUXSajhJD7skrn0l9Ri9eMAxj85/pmHr66sY3h
1tpWafP9ZT36S8ySyuwsC9/OIN8j6M2KDSt03s00yJTL0pbo8g0CgYEAmamaGEs8GxIGalM4We9b
tZAwOdZMd8cSabx8VdgdW+mdzYGVlz6qMUycI7YHjVoE46IzbU2mIRYBUAX3L7G8mP5Aco/j8Ao5
Nuh9XS1z6W/CisbRRasnjkLGqogHkC1MRTd2348QqTQgtgUcg8Mra6tjxAof8tyeiaa0aJ3vG78C
gYEA07yh3F+01RTh7BUYXs2I6WASyl6OQJGapN4eUA7pbrQTQbLOUhsUIWsVr4JDv34trd1YjI9p
60SreoErOJBUpaJIDQRRGX3QscQWAT+7zkERuvLX3iI3OFEAHyi6JEGyNhcJc3QlVhTewDqerhKL
hWQv6ev3ntHXrmiV1pJRxyECgYA6a3SeT9wmHpA51DHUX1/qg0sSchrYXuLtOC+9I1DmJMdN3jpV
KgnifFHQceAlKVg6guwyXhcO9SLCncIAa/5b3C38YCA0nm5qJbGjvygWU9sOj8/4QL3lJBYLt3PI
qLAakJ+tFuMqsRrOmNribU0QvjRLz92do6rSgoKMU58YWQKBgCPHZPEskyki2sMAH+BM1XK8P7io
Lf2/eZuXBkjvRhpvYClwCIUS+8MAL0OGub/pa48WwvLvsTQIfP9kmLK6eHt67r4WHGyiFuYJNfDj
vPAXvGpuYV5SN309JjSTpHutkckvw68mf2EYI7TV6sKdl0Ctv68eYE4yTjb8xn7AhOXl
-----END RSA PRIVATE KEY-----"""

        self.admin_certificate =  """-----BEGIN CERTIFICATE-----
MIIEVDCCAzygAwIBAgICBZ0wDQYJKoZIhvcNAQELBQAwazETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRww
GgYDVQQDExNDSUxvZ29uIE9wZW5JRCBDQSAxMB4XDTExMDgxMDIzMzQxOFoXDTExMDgxMTExMzkx
OFowZjETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMQ8wDQYDVQQKEwZHb29nbGUxGDAWBgNVBAMTD09PSS1DSSBPT0kgQTUwMTCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAIDLZVG6oyn3sHlb9Xg/s0+09guSQRiIngNJh8Fxd02G
DKye0et/sfjO358Evq8NSeRx9lgbWFeBYtRqg4enxz913FySUXh7WxYjgm72No9aeMtY3DhwihrI
hvvpIrnZH5upAr+v8N/NgrSXmSZfEsO/VhW8WzjtnbCPhrgeP+3s8u6k/jZrJly03T76Lh7OfY+D
oiio1aEJ7zp077JN3FRcKXH/9WbM5dnT0sWj8gtsRfA0oUpTLr9Pi7ukwN/3bb1aGby6m5FzJzZZ
TD/Bql7BZs1dnIoKK3C0rXWQ/1w7XUxKMfUEAcAdLemMc+fJlmLlkj7ceu3qIiqacxD8gskCAwEA
AaOCAQUwggEBMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoGCCsGAQUF
BwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAwMwbAYDVR0fBGUwYzAvoC2gK4YpaHR0cDovL2Ny
bC5jaWxvZ29uLm9yZy9jaWxvZ29uLW9wZW5pZC5jcmwwMKAuoCyGKmh0dHA6Ly9jcmwuZG9lZ3Jp
ZHMub3JnL2NpbG9nb24tb3BlbmlkLmNybDBEBgNVHREEPTA7gRFteW9vaWNpQGdtYWlsLmNvbYYm
dXJuOnB1YmxpY2lkOklETitjaWxvZ29uLm9yZyt1c2VyK0E1MDEwDQYJKoZIhvcNAQELBQADggEB
AJVbyOSd5TI/ZJFTWjKzyzoRWCsVsa0Kc+8mZb48fqIh+aWIeQb232uzcb6E3d6c7EHy3sYx/mGX
p1yNw0tYl4bMmj3DCZNvOpl3fiI29a351nmMOmbCgWETSTOUr+Peoc90fwa77MJ+u4P/8m95vJpf
IkUze92bJ78k8ztmVCw69R7DTooNMLc2GH3zcdp3ul+4We/uIV0VvQPQnqKAibUb2spHjU1/u6Kw
aIJVedgVu050DzA/gyv019p1tzJAHsaz4fwpd5iSelmOHU2ZCIIRPz9uRHQLQfVq1C4lzVdhogby
wyHT0uL94u2u3IELKAcY8Zz78hdHv2AWwpwenMk=
-----END CERTIFICATE-----"""

        self.admin_rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAgMtlUbqjKfeweVv1eD+zT7T2C5JBGIieA0mHwXF3TYYMrJ7R63+x+M7fnwS+
rw1J5HH2WBtYV4Fi1GqDh6fHP3XcXJJReHtbFiOCbvY2j1p4y1jcOHCKGsiG++kiudkfm6kCv6/w
382CtJeZJl8Sw79WFbxbOO2dsI+GuB4/7ezy7qT+NmsmXLTdPvouHs59j4OiKKjVoQnvOnTvsk3c
VFwpcf/1Zszl2dPSxaPyC2xF8DShSlMuv0+Lu6TA3/dtvVoZvLqbkXMnNllMP8GqXsFmzV2cigor
cLStdZD/XDtdTEox9QQBwB0t6Yxz58mWYuWSPtx67eoiKppzEPyCyQIDAQABAoIBABdlW01iavtX
rC4Pf2LNp4QGKl/lvH95acLNG6UPOI3TmP/OhfGSq8C3y7V2RjFEZ7Tg4tAUf5K9xTcy9huxZado
gJQsXDJXri8yWiJQBY867xB5Xt+9ycidvq+KJS2/fFdpdz9c9ZOiIGkv1Lk8sgru+fNO2P9ZYrjN
Cbrue8x7aZERyxuewU7opU6BihX2Ckw+YLGQwTYmqp6nNDKXV3dZY9tKE4a9uuhOelPggbi1Zy98
/HrN6qMqH6ICQq3Zm82NZQlb8PL0u6v18Ojf+4BUDKyD9QHWtuB7IsUnDU3Wr1Y0Rpi0hx5gXM4/
2QwzOqVS3n6BSCJCOimwqkKOh4UCgYEAu2i4+Q1VGazIW98/KKEEL87gPDxXIhq1IcbzfzMAqsNp
bxGdypMmD7fnHN8FEFqTw8UND9DOsmh90fSPmprrXoLlf4Ymr3DNKhrZVKzrOM/LB0L6Yry3NXWN
2cZE5bj+0bxLqqQ2rFxoEfWjilO0lLUOW7XFr/PLTNCd9xmK4LMCgYEAr+7JgMwQi9LTO+oF+F0J
oExwoSqre8SbtGoGvke0Bb+ir/RD1Ghoa01PgX0xM1XRhaeMjLoVj4/EDcSMKC5gS+Lug9VRUbZt
drz6f0lcvy39rzL7yEoB/Ap2QXRLmILUs14zR/duOiAA/cqm49uCzp4hypRcmepvw4beL9FSlJMC
gYEAshb0IAehZQKia1ucozlPxzaqM9OLYadLlUuAPNH0wlFsMdXlwolO1AUIpJDyOPY6EQGCRhNB
OJy/Y/MpO9wX6vosqKCMxo9FB8v31tVzucsMvlvRoF6BI1YQdHBLLJo93IU4ynG+WtB9PQPWYy7k
HaRofpIfx/K+sMJWOmiVZq0CgYBqoKav0P4WQGiV33hO1tSGus1oYJweH0LfTYNYv8xzz3miesDB
c6YVon2VVXMEUfbysmGUyRNYNyHz1jO8Bp+GXruAW0E17QLa/B42FxiHJjCihpvjADfDsfOKKBnJ
DUIsk+Mwst2zjMIND02mu9vDrkN8q/6TqmqibpMrGAqc0QKBgAt4sl2UD6ZthrElIe6+R7zGvJtq
dygQsBAF2wb2sluIRu6zdm9EDp6XzlzouN3PhCr6Vh/CO/zf43+dKC6yrRDAFiEttoOgeMTBIj2B
xN/XW/CKTL9f05RpuHULaoNHcOqi56m+Cs6jUaOuVwPxn5T0YoN/Tp24hbRfxFvMG34V
-----END RSA PRIVATE KEY-----"""

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_CRUD_resource_requests(self):
        # Preliminary test setup to get OOI ID of user with DATA_PROVIDER role,
        # user with MARINE_OPERATOR role and user with ADMIN role.
        identity_request1 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request1')
        identity_request1.configuration = identity_request1.CreateObject(IDENTITY_TYPE)
        identity_request1.configuration.certificate = self.user1_certificate
        identity_request1.configuration.rsa_private_key = self.user1_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response1 = yield self.irc.register_user(identity_request1)
        data_provider_ooi_id = identity_response1.resource_reference.ooi_id

        identity_request2 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request2')
        identity_request2.configuration = identity_request2.CreateObject(IDENTITY_TYPE)
        identity_request2.configuration.certificate = self.user2_certificate
        identity_request2.configuration.rsa_private_key = self.user2_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response2 = yield self.irc.register_user(identity_request2)
        marine_operator_ooi_id = identity_response2.resource_reference.ooi_id

        identity_request3 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request3')
        identity_request3.configuration = identity_request3.CreateObject(IDENTITY_TYPE)
        identity_request3.configuration.certificate = self.admin_certificate
        identity_request3.configuration.rsa_private_key = self.admin_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response3 = yield self.irc.register_user(identity_request3)
        admin_ooi_id = identity_response3.resource_reference.ooi_id

        create_request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='hello_create_resource request')
        create_request.configuration = create_request.CreateObject(PROTECTED_RESOURCE_CREATE_REQ_TYPE)
        create_request.configuration.name = 'SIO Buoy 1'
        create_request.configuration.description = 'SIO sensor 1'
        
        # Should fail for ANONYMOUS
        sent = False
        try:
            result = yield self.hc.hello_create_resource(create_request)
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

        # Should fail for non-owner
        sent = False
        try:
            result = yield self.hc.hello_delete_resource(delete_request, marine_operator_ooi_id)
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)
        
        # Create bad message to test error flows.
        # Don't specify object id
        bad_delete_request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='bad_hello_delete_resource request')
        bad_delete_request.configuration = create_request.CreateObject(PROTECTED_RESOURCE_DELETE_REQ_TYPE)

        # Should fail when object id not specified (can't look up association)
        sent = False
        try:
            result = yield self.hc.hello_delete_resource(bad_delete_request, data_provider_ooi_id)
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)
        
        # Specify empty object id.
        bad_delete_request.configuration.resource_ids.append('')

        # Should fail when object id not specified (can't look up association)
        sent = False
        try:
            result = yield self.hc.hello_delete_resource(bad_delete_request, data_provider_ooi_id)
            sent = True
        except ReceivedApplicationError, ex:
            pass
        self.assertFalse(sent)
        
        # Specify bogus object id.
        bad_delete_request.configuration.resource_ids.append('Bogus')

        # Should fail when object id not specified (can't look up association)
        sent = False
        try:
            result = yield self.hc.hello_delete_resource(bad_delete_request, data_provider_ooi_id)
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

    @defer.inlineCallbacks
    def test_bad_msg_format(self):
        # Very specific tests to validate bad message format flows.
        # Requires calling directly into the policy interceptor because
        # the message infrastructure won't allow it.
        invocation = Invocation()
        pi = PolicyInterceptor('foo')
        
        # Missing message header params
        yield pi.is_authorized({'performative':'request'},invocation)
        self.assertTrue(invocation.status != Invocation.STATUS_PROCESS)

        yield pi.is_authorized({'performative':'request', 'user-id': 'ABC'},invocation)
        self.assertTrue(invocation.status != Invocation.STATUS_PROCESS)

        yield pi.is_authorized({'performative':'request', 'user-id': 'ABC', 'expiry': '0'},invocation)
        self.assertTrue(invocation.status != Invocation.STATUS_PROCESS)

        yield pi.is_authorized({'performative':'request', 'user-id': 'ABC', 'expiry': '0', 'receiver': 'foo'},invocation)
        self.assertTrue(invocation.status != Invocation.STATUS_PROCESS)

        # Malformed expiry
        yield pi.is_authorized({'performative':'request', 'user-id': 'ABC', 'expiry': 0, 'receiver': 'foo', 'op': 'foo'},invocation)
        self.assertTrue(invocation.status != Invocation.STATUS_PROCESS)

        yield pi.is_authorized({'performative':'request', 'user-id': 'ABC', 'expiry': 'ABC', 'receiver': 'foo', 'op': 'foo'},invocation)
        self.assertTrue(invocation.status != Invocation.STATUS_PROCESS)


    @defer.inlineCallbacks
    def test_user_roles(self):
        # Register users with diffent roles
        # Data Provider
        identity_request1 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request1')
        identity_request1.configuration = identity_request1.CreateObject(IDENTITY_TYPE)
        identity_request1.configuration.certificate = self.user1_certificate
        identity_request1.configuration.rsa_private_key = self.user1_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response1 = yield self.irc.register_user(identity_request1)
        data_provider_ooi_id = identity_response1.resource_reference.ooi_id

        authentication = Authentication()

        cert_info = authentication.decode_certificate(str(self.user1_certificate))
        data_provider_subject = cert_info['subject']

        # Marine operator
        identity_request2 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request2')
        identity_request2.configuration = identity_request2.CreateObject(IDENTITY_TYPE)
        identity_request2.configuration.certificate = self.user2_certificate
        identity_request2.configuration.rsa_private_key = self.user2_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response2 = yield self.irc.register_user(identity_request2)
        marine_operator_ooi_id = identity_response2.resource_reference.ooi_id

        cert_info = authentication.decode_certificate(str(self.user2_certificate))
        marine_operator_subject = cert_info['subject']

        # Super user
        identity_request3 = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request3')
        identity_request3.configuration = identity_request3.CreateObject(IDENTITY_TYPE)
        identity_request3.configuration.certificate = self.admin_certificate
        identity_request3.configuration.rsa_private_key = self.admin_rsa_private_key
        
        # Register a user, this shouldn't fail
        identity_response3 = yield self.irc.register_user(identity_request3)
        admin_ooi_id = identity_response3.resource_reference.ooi_id

        cert_info = authentication.decode_certificate(str(self.admin_certificate))
        admin_subject = cert_info['subject']

        self.assertTrue(subject_has_role(data_provider_subject, 'ANONYMOUS'))
        self.assertTrue(user_has_role(data_provider_ooi_id, 'ANONYMOUS'))
        self.assertTrue(subject_has_data_provider_role(data_provider_subject))
        map_ooi_id_to_subject_data_provider_role(data_provider_subject, data_provider_ooi_id)
        self.assertTrue(user_has_role(data_provider_ooi_id, 'DATA_PROVIDER'))
        self.assertFalse(subject_has_role(data_provider_subject, 'MARINE_OPERATOR'))
        self.assertTrue(subject_has_role(data_provider_subject, 'EARLY_ADOPTER'))
        self.assertFalse(subject_has_role(data_provider_subject, 'ADMIN'))

        self.assertTrue(user_has_role(marine_operator_ooi_id, 'ANONYMOUS'))
        self.assertFalse(subject_has_role(marine_operator_subject, 'DATA_PROVIDER'))
        self.assertTrue(subject_has_marine_operator_role(marine_operator_subject))
        map_ooi_id_to_subject_marine_operator_role(marine_operator_subject, marine_operator_ooi_id)
        self.assertTrue(user_has_role(marine_operator_ooi_id, 'MARINE_OPERATOR'))
        self.assertFalse(subject_has_role(marine_operator_subject, 'EARLY_ADOPTER'))
        self.assertFalse(subject_has_role(marine_operator_subject, 'ADMIN'))

        self.assertTrue(user_has_role(admin_ooi_id, 'ANONYMOUS'))
        self.assertTrue(subject_has_data_provider_role(admin_subject))
        map_ooi_id_to_subject_data_provider_role(admin_subject, admin_ooi_id)
        self.assertTrue(user_has_data_provider_role(admin_ooi_id))
        self.assertTrue(subject_has_marine_operator_role(admin_subject))
        map_ooi_id_to_subject_marine_operator_role(admin_subject, admin_ooi_id)
        self.assertTrue(user_has_marine_operator_role(admin_ooi_id))
        self.assertTrue(subject_has_early_adopter_role(admin_subject))
        map_ooi_id_to_subject_early_adopter_role(admin_subject, admin_ooi_id)
        self.assertTrue(user_has_early_adopter_role(admin_ooi_id))
        self.assertTrue(subject_has_admin_role(admin_subject))
        map_ooi_id_to_subject_admin_role(admin_subject, admin_ooi_id)
        self.assertTrue(user_has_admin_role(admin_ooi_id))
        