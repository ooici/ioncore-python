#!/usr/bin/env python

"""
@file ion/services/coi/test/test_registration.py
@author Roger Unwin
@brief test service for registering users
"""

import logging
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
        logging.debug('+++++++++++++++++DONE SETUP')

       

       
        
    @defer.inlineCallbacks
    def tearDown(self):
        logging.debug('+++++++++++++++++START TEARDOWN')
        yield self._stop_container()
        logging.debug('+++++++++++++++++DONE TEARDOWN')
        
    @defer.inlineCallbacks
    def test_register_user(self):
        logging.debug('+++++++++++++++++START test_RegisterUser')
        """rs = IdentityRegistryService()
        """
        services = [{'name':'register_user',
                     'module':'ion.services.coi.identity_registry',
                     'class': 'IdentityRegistryService'},]
        sup = yield self._spawn_processes(services)

        hc = IdentityRegistryServiceClient(proc=sup)
        yield hc.register_user("Hi there")

