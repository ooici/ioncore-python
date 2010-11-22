#!/usr/bin/env python

"""
@file ion/services/coi/test/test_identity_registry.py
@author Roger Unwin
@brief test service for registering users
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.core.exception import ReceivedError
from ion.test.iontest import IonTestCase
from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.resources import coi_resource_descriptions

class UserRegistrationClientTest(IonTestCase):
    """
    Testing client classes of User Registration
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        """
        yield self._start_container()

        services = [{'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}]
        supervisor = yield self._spawn_processes(services)

        self.identity_registry_client = IdentityRegistryClient(proc=supervisor)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.identity_registry_client.clear_identity_registry()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_register_user(self):
        """
        """
    

        user = coi_resource_descriptions.IdentityResource.create_new_resource()

        # initialize the user
        #user.common_name = "Roger Unwin A13"
        #user.country = "US"
        #user.trust_provider = "ProtectNetwork"
        #user.domain_component = "cilogon"
        user.subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254"
        user.certificate =  """-----BEGIN CERTIFICATE-----
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
        user.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
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
        #user.expiration_date = "Tue Jun 29 23:32:16 PDT 2010"
        # These are the fields we prompt the user for during registration
        #user.first_name = "Roger"
        #user.last_name = "Unwin"
        #user.phone = "8588675309"
        #user.fax = "6198675309"
        #user.email = "unwin@sdsc.edu"
        #user.organization = "University of California San Diego"
        #user.department = "San Diego Supercomputing Center"
        #user.title = "Deep Sea Submarine Captain"


        #-----------------

        ooi_id = yield self.identity_registry_client.register_user_credentials(user.certificate, user.rsa_private_key)
        print ooi_id
        print "#"
        print "#"
        print "#"
        print "#"
        print "#"
        print "#"
        #-----------------

        user = yield self.identity_registry_client.register_user(user)
        
        ooi_id = user.reference()
        print str(ooi_id)
        print "saved and got this id back " + str(ooi_id.RegistryIdentity)
        # load the user back
        user0 = yield self.identity_registry_client.get_user(ooi_id)

        # Test that we got a Person back
        self.assertNotEqual(user0, None)
        self.assertEqual(user0.subject, "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254")

        # Test the ooi_id was properly set within the Person object
        self.assertEqual(user0.reference(), ooi_id)

        # Test that updates work
        user0.subject = "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254 CHANGED"
        user0 = yield self.identity_registry_client.update_user(user0)
        ooi_id = user0.reference()

        
        
        user1 = yield self.identity_registry_client.get_user(ooi_id)
        #self.assertEqual("Roger Unwin CHANGED", user1.common_name)
        self.assertEqual(user1.subject, "/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254 CHANGED")

        # Test for user not found handled properly.
        ooi_id.RegistryIdentity = "bogus-ooi_id"
        try:
            result = yield self.identity_registry_client.get_user(ooi_id)
            self.fail("ReceivedError expected")
        except ReceivedError, re:
            log.error('Above error "WARNING:RPC reply is an ERROR: None" is expected.')
            pass

        # Test if we can find the user we have stuffed in.
        user_description = coi_resource_descriptions.IdentityResource()
        user_description.subject = 'oger'

        users1 = yield self.identity_registry_client.find_users(user_description,regex=True)
        self.assertEqual(len(users1), 1) # should only return 1 match
        self.assertEqual("/DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A254 CHANGED", users1[0].subject)

        # Test if we can set the life cycle state
        self.assertEqual(str(user1.lifecycle), 'new') # Should start as new

        ooi_id = user0.reference(head=True)

        result = yield self.identity_registry_client.set_identity_lcstate_retired(ooi_id) # Wishful thinking Roger!
        user2 = yield self.identity_registry_client.get_user(ooi_id)
        self.assertEqual(str(user2.lifecycle), 'retired') # Should be retired now
