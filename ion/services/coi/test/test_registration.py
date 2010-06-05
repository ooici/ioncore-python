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
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.authenticate("TESTING")
        logging.debug('### Service reply: '+result['value'])
        
    @defer.inlineCallbacks
    def test_generate_ooi_id(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.generate_ooi_id("TESTING")
        logging.debug('### Service reply: '+result['value'])
        
    @defer.inlineCallbacks
    def test_revoke_ooi_id(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.revoke_ooi_id("TESTING")
        logging.debug('### Service reply: '+result['value'])
         
    @defer.inlineCallbacks
    def test_store_registration(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.store_registration("TESTING")
        logging.debug('### Service reply: '+result['value'])
               
    @defer.inlineCallbacks
    def test_store_registration_info(self):
        """
        """
        
        hc = IdentityRegistryServiceClient(proc=self.sup)
        result = yield hc.store_registration_info("TESTING")
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
