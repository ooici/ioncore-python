#!/usr/bin/env python

"""
@file ion/services/coi/test/test_registration.py
@author Roger Unwin
@brief test service for registering users
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.test.iontest import IonTestCase
from ion.services.coi.identity_registry import IdentityRegistryService, IdentityRegistryClient, Person




class UserRegistrationClientTest(IonTestCase):
    """
    Testing client classes of User Registration
    """
    
    @defer.inlineCallbacks
    def setUp(self):
        
        yield self._start_container()

        services = [{'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}]
        supervisor = yield self._spawn_processes(services)
        
        
        self.identity_registry_client = IdentityRegistryClient(proc=supervisor)
        
    @defer.inlineCallbacks
    def test_register_user(self):
        """
        """
        
        
        user = Person()

        # initialize the user
        user.common_name = "Roger Unwin A13"
        user.country = "US" 
        user.trust_provider = "ProtectNetwork"
        user.domain_component = "cilogon"
        user.certificate =  "MIIEMjCCAxqgAwIBAgIBZDANBgkqhkiG9w0BAQUFADBqMRMwEQYKCZImiZPyLGQB\n" + \
                            "GRYDb3JnMRcwFQYKCZImiZPyLGQBGRYHY2lsb2dvbjELMAkGA1UEBhMCVVMxEDAO\n" + \
                            "BgNVBAoTB0NJTG9nb24xGzAZBgNVBAMTEkNJTG9nb24gQmFzaWMgQ0EgMTAeFw0x\n" + \
                            "MDA2MjkxODIxNTlaFw0xMDA2MzAwNjI2NTlaMG8xEzARBgoJkiaJk/IsZAEZEwNv\n" + \
                            "cmcxFzAVBgoJkiaJk/IsZAEZEwdjaWxvZ29uMQswCQYDVQQGEwJVUzEXMBUGA1UE\n" + \
                            "ChMOUHJvdGVjdE5ldHdvcmsxGTAXBgNVBAMTEFJvZ2VyIFVud2luIEExMzYwggEi\n" + \
                            "MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQD0Ci/5ZK1Cta7OaVWGCJb3js/p\n" + \
                            "l+0B4AHXjqOTcSDX/f7U+kful3cKhcPryIV2/bnfh9dYpC4RNHVI32uACH/BCkFG\n" + \
                            "kqsNrLfh7b8g41xBxnREwANI/NEqzwcgNL9mfi8SiY8lEOxGqMYdsNo20QsehRgo\n" + \
                            "SPswGAA0uApdBGkxJGolaPscG7z10WQEd0/HUiAnda8RP0QEPmqPvX/2xJT9sgOg\n" + \
                            "KEU/to+ue/eBuwrlTjy4qn3IsGlwKyckXe9wXnkmck/S9MWvEax17cC4qjTZKYpy\n" + \
                            "/k0NMBVcO+dsqdGuwus4q4IxBHy9P8kq3QAQoJies11RspTCQ61GuVwCu1V7AgMB\n" + \
                            "AAGjgd0wgdowDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCBLAwEwYDVR0lBAww\n" + \
                            "CgYIKwYBBQUHAwIwGAYDVR0gBBEwDzANBgsrBgEEAYKRNgECATBqBgNVHR8EYzBh\n" + \
                            "MC6gLKAqhihodHRwOi8vY3JsLmNpbG9nb24ub3JnL2NpbG9nb24tYmFzaWMuY3Js\n" + \
                            "MC+gLaArhilodHRwOi8vY3JsLmRvZWdyaWRzLm9yZy9jaWxvZ29uLWJhc2ljLmNy\n" + \
                            "bDAfBgNVHREEGDAWgRRpdHNhZ3JlZW4xQHlhaG9vLmNvbTANBgkqhkiG9w0BAQUF\n" + \
                            "AAOCAQEAT0uKY2BEYPhozSBzRrOgY9gw5yXczbq3kEx+kxF1jXAeMMD3u2i1apZ3\n" + \
                            "dIS0FzCj1b3y8tfBr5FNde//axLZfxSo3kO9djlUrfqJnycaGevc2zMD8+GIst0J\n" + \
                            "OB+3GClYYpagLkRbkkLX3hU/qfb4c4DnHVmZOaXKsOKETB7xcxWimIlL47O2XGNt\n" + \
                            "PEUP0RzlJTkgD7LeSS0I+9SlFe0Wdb6knkMJ4afT5xLr2FXkcU9VRqGu8Gr2A4ha\n" + \
                            "CKLqTUDKFOUp5i16pLY4p6Ahn0IcFWOyWJLQ70mUJz+WVqCVXjkfMpKbrMgcKZNC\n" + \
                            "9kgKHA8cRnz97xbQIcDdeGU9tCAuGQ=="
        user.rsa_private_key = "MIIEowIBAAKCAQEA9Aov+WStQrWuzmlVhgiW947P6ZftAeAB146jk3Eg1/3+1PpH\n" + \
                            "7pd3CoXD68iFdv2534fXWKQuETR1SN9rgAh/wQpBRpKrDay34e2/IONcQcZ0RMAD\n" + \
                            "SPzRKs8HIDS/Zn4vEomPJRDsRqjGHbDaNtELHoUYKEj7MBgANLgKXQRpMSRqJWj7\n" + \
                            "HBu89dFkBHdPx1IgJ3WvET9EBD5qj71/9sSU/bIDoChFP7aPrnv3gbsK5U48uKp9\n" + \
                            "yLBpcCsnJF3vcF55JnJP0vTFrxGsde3AuKo02SmKcv5NDTAVXDvnbKnRrsLrOKuC\n" + \
                            "MQR8vT/JKt0AEKCYnrNdUbKUwkOtRrlcArtVewIDAQABAoIBACkHFW2uOVq/xLW7\n" + \
                            "C7/O7eKMxfOVsSjhii29M070c/scHp2bvkAkgsToHDolqhqJKZik89VZNM17rkQk\n" + \
                            "G6SYyTGhEbxVqCBSa0+2cq2Ky9XbEW0FgwfgSSITUDVf6NXIXQ2WxtQKdk6izTvs\n" + \
                            "oaMZne7xnVAYhPJe9pnmXweoWC8Ehiu2MrSYQHsgc7A4zSHvKscvdGkGYif6bCVx\n" + \
                            "bi1qWvEJyIlQqRW1PmtI65tmf7Y3yOnDJYBs5hQ7nSVgSFIwox8L9h1QhU72E9kN\n" + \
                            "aqMakS5qS/PnTilB17xfbrT6Gx9yTBlm4Zyc2JgqIRjICRYetYRzQYRVv8v262D8\n" + \
                            "nHIv8CECgYEA/3RDBV9mgXwrN9ol+ICfG4SeJHzTfVJjGB+GsPIZ0yUQ3rpKBVvA\n" + \
                            "/eXBfEPL+ruDMSt5y/KnqXQu3JhiRP+uOgfwUK6GMRAJS8XkVr98nM/cW646dnvQ\n" + \
                            "k1S9LOmqPkiTq6VnOJIzwmTtUCtwpqwU8ZbuZHKagGhMCVGzrCJgC0MCgYEA9I+u\n" + \
                            "hhlkZaMrzLOTtti+5dGVtBy+avSqkp2f4oQG/Z62NGSnxzNqELnriErZkYR3d4Kv\n" + \
                            "zj0j+CO5V9xLpYzHusgYk7jLRcZQSyVjnkkrorOT2qeBqgl/FYSoD64+Eydq+NsV\n" + \
                            "7xwWFCWbcmDAFxPJU2eqES85cQSiAzi6d1ndfWkCgYAXcBZaHter17WraTOEqmBu\n" + \
                            "yOstk9pfrDh1VScpgv0Fl2gF13fFKBb79KGdAidr+Npfn4qMQNZLQOKv0Ldrdz4I\n" + \
                            "CwRskqazR7JipmR95RHM3XFtY/3vMwr/CY5V2ZaKImSSIhnnYdqn4lS3v1SVpkJB\n" + \
                            "rERxKOauE2OukzV1/K1tOwKBgQCAZRrETnp2Hdd17eWkPmDiuUj2OY0DDBatSNHT\n" + \
                            "E2u0JWoVUa8AFw8dXu64LEvTaQ9rkBIKnfDParn41bBlZubJOholHASkSjyHZ0bI\n" + \
                            "qDOfhNYgGocppTiyLGYrbVgrqCsyIZt/YGh7BU96Gi9fLkUpY6hWw0tN+ZexR0wm\n" + \
                            "Mujk2QKBgAugycaF1zFmK7UOg9/thfY07uNgnYRFLDCYB4G5f0xeagzYp+SEt3oO\n" + \
                            "q23C/NhdxAQH1rfUptZXM1rnVyM+ycI3IulnAiJ96AD5FQX0PRWmo2DtHgpZ21Sh\n" + \
                            "cZGidYECf6XdGxhSsf2C81LcDdk99KlPd7fkqLWOs5cAwlR4r3CY"
        user.expiration_date = "Tue Jun 29 23:32:16 PDT 2010"
        # These are the fields we prompt the user for during registration
        user.first_name = "Roger"
        user.last_name = "Unwin"
        user.phone = "8588675309"
        user.fax = "6198675309"
        user.email = "unwin@sdsc.edu"
        user.organization = "University of California San Diego"
        user.department = "San Diego Supercomputing Center"
        user.title = "Deep Sea Submarine Captain"
        
        
    
        ooi_id = yield self.identity_registry_client.register_user(user)

        
        user0 = yield self.identity_registry_client.get_user(ooi_id)
        # Test that we got a Person back
        self.assertNotEqual(user0, None)
        self.assertEqual(user0.common_name, "Roger Unwin A13")
        
        # Test the ooi_id was properly set within the Person object
        self.assertEqual(user0.ooi_id, ooi_id)
        #logging.debug('###2 Service reply: ##################################### (Should be "Roger Unwin A13"'+ result.common_name)
        
        # Test that updates work
        user0.common_name = "Roger Unwin CHANGED"
        ooi_id = yield self.identity_registry_client.update_user(user0)
        user1 = yield self.identity_registry_client.get_user(ooi_id)
        self.assertEqual("Roger Unwin CHANGED", user1.common_name)
        
        # Test for user not found handled properly.
        result = yield self.identity_registry_client.get_user("bogus-ooi_id")
        self.assertEqual(result, None)
        
        # Test if we can find the user we have stuffed in.
        users1 = yield self.identity_registry_client.find_users({'first_name': "oger"})
        self.assertEqual(len(users1), 1) # should only return 1 match
        self.assertEqual("Roger Unwin CHANGED", users1[0].common_name)
        
        # Test if we can set the life cycle state
        self.assertEqual(str(user1.lifecycle), 'new') # Should start as new
        result = yield self.identity_registry_client.set_lcstate_retired(ooi_id)
        self.assertEqual(result, True) # should return True on success
        user2 = yield self.identity_registry_client.get_user(ooi_id)
        self.assertEqual(str(user2.lifecycle), 'retired') # Should be retired now
        logging.debug('###7 checking change lcstate: ##################################### '+ str(result))
        
        
    #@defer.inlineCallbacks
    #def test_get_registration_info(self):
    #    """
    #    """
    #    
    #    result = yield self.identity_registry_service_client.register_user("TESTING")
    #    logging.debug('### Service reply: #####################################' + result)
    #    result = yield self.identity_registry_service_client.get_registration_info("TESTING")
    #    logging.debug('### Service reply: #####################################'+ str(result))
    """
    @defer.inlineCallbacks
    def test_update_user(self):
        
        user = Person()

        # initialize the user
        user.common_name = "Roger Unwin A13"
        user.country = "US" 
        user.trust_provider = "ProtectNetwork"
        user.domain_component = "cilogon"
        user.certificate =  "MIIEMjCCAxqgAwIBAgIBZDANBgkqhkiG9w0BAQUFADBqMRMwEQYKCZImiZPyLGQB\n" + \
                            "GRYDb3JnMRcwFQYKCZImiZPyLGQBGRYHY2lsb2dvbjELMAkGA1UEBhMCVVMxEDAO\n" + \
                            "BgNVBAoTB0NJTG9nb24xGzAZBgNVBAMTEkNJTG9nb24gQmFzaWMgQ0EgMTAeFw0x\n" + \
                            "MDA2MjkxODIxNTlaFw0xMDA2MzAwNjI2NTlaMG8xEzARBgoJkiaJk/IsZAEZEwNv\n" + \
                            "cmcxFzAVBgoJkiaJk/IsZAEZEwdjaWxvZ29uMQswCQYDVQQGEwJVUzEXMBUGA1UE\n" + \
                            "ChMOUHJvdGVjdE5ldHdvcmsxGTAXBgNVBAMTEFJvZ2VyIFVud2luIEExMzYwggEi\n" + \
                            "MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQD0Ci/5ZK1Cta7OaVWGCJb3js/p\n" + \
                            "l+0B4AHXjqOTcSDX/f7U+kful3cKhcPryIV2/bnfh9dYpC4RNHVI32uACH/BCkFG\n" + \
                            "kqsNrLfh7b8g41xBxnREwANI/NEqzwcgNL9mfi8SiY8lEOxGqMYdsNo20QsehRgo\n" + \
                            "SPswGAA0uApdBGkxJGolaPscG7z10WQEd0/HUiAnda8RP0QEPmqPvX/2xJT9sgOg\n" + \
                            "KEU/to+ue/eBuwrlTjy4qn3IsGlwKyckXe9wXnkmck/S9MWvEax17cC4qjTZKYpy\n" + \
                            "/k0NMBVcO+dsqdGuwus4q4IxBHy9P8kq3QAQoJies11RspTCQ61GuVwCu1V7AgMB\n" + \
                            "AAGjgd0wgdowDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCBLAwEwYDVR0lBAww\n" + \
                            "CgYIKwYBBQUHAwIwGAYDVR0gBBEwDzANBgsrBgEEAYKRNgECATBqBgNVHR8EYzBh\n" + \
                            "MC6gLKAqhihodHRwOi8vY3JsLmNpbG9nb24ub3JnL2NpbG9nb24tYmFzaWMuY3Js\n" + \
                            "MC+gLaArhilodHRwOi8vY3JsLmRvZWdyaWRzLm9yZy9jaWxvZ29uLWJhc2ljLmNy\n" + \
                            "bDAfBgNVHREEGDAWgRRpdHNhZ3JlZW4xQHlhaG9vLmNvbTANBgkqhkiG9w0BAQUF\n" + \
                            "AAOCAQEAT0uKY2BEYPhozSBzRrOgY9gw5yXczbq3kEx+kxF1jXAeMMD3u2i1apZ3\n" + \
                            "dIS0FzCj1b3y8tfBr5FNde//axLZfxSo3kO9djlUrfqJnycaGevc2zMD8+GIst0J\n" + \
                            "OB+3GClYYpagLkRbkkLX3hU/qfb4c4DnHVmZOaXKsOKETB7xcxWimIlL47O2XGNt\n" + \
                            "PEUP0RzlJTkgD7LeSS0I+9SlFe0Wdb6knkMJ4afT5xLr2FXkcU9VRqGu8Gr2A4ha\n" + \
                            "CKLqTUDKFOUp5i16pLY4p6Ahn0IcFWOyWJLQ70mUJz+WVqCVXjkfMpKbrMgcKZNC\n" + \
                            "9kgKHA8cRnz97xbQIcDdeGU9tCAuGQ=="
        user.rsa_private_key = "MIIEowIBAAKCAQEA9Aov+WStQrWuzmlVhgiW947P6ZftAeAB146jk3Eg1/3+1PpH\n" + \
                            "7pd3CoXD68iFdv2534fXWKQuETR1SN9rgAh/wQpBRpKrDay34e2/IONcQcZ0RMAD\n" + \
                            "SPzRKs8HIDS/Zn4vEomPJRDsRqjGHbDaNtELHoUYKEj7MBgANLgKXQRpMSRqJWj7\n" + \
                            "HBu89dFkBHdPx1IgJ3WvET9EBD5qj71/9sSU/bIDoChFP7aPrnv3gbsK5U48uKp9\n" + \
                            "yLBpcCsnJF3vcF55JnJP0vTFrxGsde3AuKo02SmKcv5NDTAVXDvnbKnRrsLrOKuC\n" + \
                            "MQR8vT/JKt0AEKCYnrNdUbKUwkOtRrlcArtVewIDAQABAoIBACkHFW2uOVq/xLW7\n" + \
                            "C7/O7eKMxfOVsSjhii29M070c/scHp2bvkAkgsToHDolqhqJKZik89VZNM17rkQk\n" + \
                            "G6SYyTGhEbxVqCBSa0+2cq2Ky9XbEW0FgwfgSSITUDVf6NXIXQ2WxtQKdk6izTvs\n" + \
                            "oaMZne7xnVAYhPJe9pnmXweoWC8Ehiu2MrSYQHsgc7A4zSHvKscvdGkGYif6bCVx\n" + \
                            "bi1qWvEJyIlQqRW1PmtI65tmf7Y3yOnDJYBs5hQ7nSVgSFIwox8L9h1QhU72E9kN\n" + \
                            "aqMakS5qS/PnTilB17xfbrT6Gx9yTBlm4Zyc2JgqIRjICRYetYRzQYRVv8v262D8\n" + \
                            "nHIv8CECgYEA/3RDBV9mgXwrN9ol+ICfG4SeJHzTfVJjGB+GsPIZ0yUQ3rpKBVvA\n" + \
                            "/eXBfEPL+ruDMSt5y/KnqXQu3JhiRP+uOgfwUK6GMRAJS8XkVr98nM/cW646dnvQ\n" + \
                            "k1S9LOmqPkiTq6VnOJIzwmTtUCtwpqwU8ZbuZHKagGhMCVGzrCJgC0MCgYEA9I+u\n" + \
                            "hhlkZaMrzLOTtti+5dGVtBy+avSqkp2f4oQG/Z62NGSnxzNqELnriErZkYR3d4Kv\n" + \
                            "zj0j+CO5V9xLpYzHusgYk7jLRcZQSyVjnkkrorOT2qeBqgl/FYSoD64+Eydq+NsV\n" + \
                            "7xwWFCWbcmDAFxPJU2eqES85cQSiAzi6d1ndfWkCgYAXcBZaHter17WraTOEqmBu\n" + \
                            "yOstk9pfrDh1VScpgv0Fl2gF13fFKBb79KGdAidr+Npfn4qMQNZLQOKv0Ldrdz4I\n" + \
                            "CwRskqazR7JipmR95RHM3XFtY/3vMwr/CY5V2ZaKImSSIhnnYdqn4lS3v1SVpkJB\n" + \
                            "rERxKOauE2OukzV1/K1tOwKBgQCAZRrETnp2Hdd17eWkPmDiuUj2OY0DDBatSNHT\n" + \
                            "E2u0JWoVUa8AFw8dXu64LEvTaQ9rkBIKnfDParn41bBlZubJOholHASkSjyHZ0bI\n" + \
                            "qDOfhNYgGocppTiyLGYrbVgrqCsyIZt/YGh7BU96Gi9fLkUpY6hWw0tN+ZexR0wm\n" + \
                            "Mujk2QKBgAugycaF1zFmK7UOg9/thfY07uNgnYRFLDCYB4G5f0xeagzYp+SEt3oO\n" + \
                            "q23C/NhdxAQH1rfUptZXM1rnVyM+ycI3IulnAiJ96AD5FQX0PRWmo2DtHgpZ21Sh\n" + \
                            "cZGidYECf6XdGxhSsf2C81LcDdk99KlPd7fkqLWOs5cAwlR4r3CY"
        user.expiration_date = "Tue Jun 29 23:32:16 PDT 2010"
        # These are the fields we prompt the user for during registration
        user.first_name = "Roger"
        user.last_name = "Unwin"
        user.phone = "8588675309"
        user.fax = "6198675309"
        user.email = "unwin@sdsc.edu"
        user.organization = "University of California San Diego"
        user.department = "San Diego Supercomputing Center"
        user.title = "Deep Sea Submarine Captain"
        
        ooi_id = yield self.identity_registry_client.register_user(user)
        logging.debug('###1 register_user service reply: #####################################' + result)
        
        user.common_name = "Roger Unwin CHANGED"
        user.phone = "8588675309 CHANGED"
        user.country = "UK CHANGED"
        
        user.ooi_id = ooi_id  # may not be needed.
        result = yield self.identity_registry_client.update_user(user)
        logging.debug('###2 update_user service reply: #####################################' + result)
        
        
        logging.debug('###Trying to get lookup for key ' + result )
        result = yield self.identity_registry_client.get_registration_info(ooi_id)
        logging.debug('###3 Service reply: #####################################'+ str(result))
        
    """
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
'''        
    @defer.inlineCallbacks
    def test_define_identity(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.define_identity("TESTING")
        logging.debug('### Service reply: ' + result['value'])
                
 
    @defer.inlineCallbacks
    def test_define_user_profile(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.define_user_profile("TESTING")
        logging.debug('### Service reply: '+result['value'])
        
    @defer.inlineCallbacks
    def test_authenticate(self):
        """
        What we get from CILogon:
        /DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A136
        CN=Roger Unwin A136, O=ProtectNetwork, C=US, DC=cilogon, DC=org
        What it means....
        /C = country
        /O = organization
        /CN = Common Name?
        /DC = Domain Component
        """
        parms = {'Common_name': 'Roger Unwin A136',
                 'Organization': 'ProtectNetwork',
                 'Domain Component': 'cilogon org',
                 'Country': 'US',
                 'Certificate': 'dummy certificate',
                 'RSA Private Key': 'dummy rsa private key'}
        
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.authenticate(parms)
        
        logging.info('### User was authenticated: '+ str(result['authenticated']))
        
        self.failUnlessEqual(result['authenticated'], True)
        
    @defer.inlineCallbacks
    def test_generate_ooi_id(self):
        """
        What we get from CILogon:
        /DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A136
        CN=Roger Unwin A136, O=ProtectNetwork, C=US, DC=cilogon, DC=org
        What it means....
        /C = country
        /O = organization
        /CN = Common Name?
        /DC = Domain Component
        """
        parms = {'Common_name': 'Roger Unwin A136',
                 'Organization': 'ProtectNetwork',
                 'Domain Component': 'cilogon org',
                 'Country': 'US',
                 'Certificate': 'dummy certificate',
                 'RSA Private Key': 'dummy rsa private key'}
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.generate_ooi_id(parms)
        logging.debug('### Generated ooi_id is : ' + str(result['ooi_id']) )
        
        if (result['ooi_id'] > 0) :
            self.assertEqual(result['ooi_id'],result['ooi_id']) # hack since I cant find a pass(). this will get revisited in later iterations. Once its generating proper ooi_id's then this should be corrected.
        else:
            self.fail(self, msg='ooi should have come back as a number')
        
    @defer.inlineCallbacks
    def test_revoke_ooi_id(self):
        """
        """
        logging.debug('### ENTERING test_revoke_ooi_id')

        parms = {'ooi_id':'unwin45872043897'}
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.revoke_ooi_id(parms)
        
        logging.debug('### Service reply: ' + str(result['revoked']))
        
        self.failUnlessEqual(result['revoked'], True)
         
    @defer.inlineCallbacks
    def test_store_registration(self):
        """
        What we get from CILogon:
        /DC=org/DC=cilogon/C=US/O=ProtectNetwork/CN=Roger Unwin A136
        CN=Roger Unwin A136, O=ProtectNetwork, C=US, DC=cilogon, DC=org
        What it means....
        /C = country
        /O = organization
        /CN = Common Name?
        /DC = Domain Component
        """
        parms = {'common_name': 'Roger Unwin A136',
                 'organization': 'ProtectNetwork',
                 'Domain Component': 'cilogon org',
                 'Country': 'US',
                 'Certificate': 'dummy certificate',
                 'RSA Private Key': 'dummy rsa private key'}
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.store_registration(parms)
        logging.debug('###2 Service reply: '+str(result['value']))
               
    @defer.inlineCallbacks
    def test_store_registration_info(self):
        """
        """
        parms = {'ooi_id':'unwin45872043897'}
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.store_registration_info(parms)
        logging.debug('### Service reply: '+result['value'])


    @defer.inlineCallbacks
    def test_update_registration_info(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.update_registration_info("TESTING")
        logging.debug('### Service reply: '+result['value'])

    @defer.inlineCallbacks
    def test_revoke_registration(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.supervisor)
        result = yield hc.revoke_registration("TESTING")
        logging.debug('### Service reply: '+result['value'])
'''
