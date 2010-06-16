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
from ion.services.coi.identity_registry import IdentityRegistryService, IdentityRegistryServiceClient

class UserRegistrationClientTest(IonTestCase):
    """
    Testing client classes of User Registration
    """
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        services = [{'name':'register_user',
                     'module':'ion.services.coi.identity_registry',
                     'class': 'IdentityRegistryService'},]
        self.sup = yield self._spawn_processes(services)

        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        
    @defer.inlineCallbacks
    def test_define_identity(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.define_identity("TESTING")
        logging.debug('### Service reply: ' + result['value'])
                
    @defer.inlineCallbacks
    def test_register_user(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.register_user("TESTING")
        logging.debug('### Service reply: ' + result['value'])
        
    @defer.inlineCallbacks
    def test_define_user_profile(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
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
        
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
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
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
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
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
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
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.store_registration(parms)
        logging.debug('###2 Service reply: '+result['value'])
               
    @defer.inlineCallbacks
    def test_store_registration_info(self):
        """
        """
        parms = {'ooi_id':'unwin45872043897'}
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.store_registration_info(parms)
        logging.debug('### Service reply: '+result['value'])

    @defer.inlineCallbacks
    def test_get_registration_info(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.get_registration_info("TESTING")
        logging.debug('### Service reply: '+result['value'])

    @defer.inlineCallbacks
    def test_update_registration_info(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.update_registration_info("TESTING")
        logging.debug('### Service reply: '+result['value'])

    @defer.inlineCallbacks
    def test_revoke_registration(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.revoke_registration("TESTING")
        logging.debug('### Service reply: '+result['value'])
