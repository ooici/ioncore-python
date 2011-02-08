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

from ion.test.iontest import IonTestCase
 
from ion.services.coi.exchange.broker_controller import BrokerController

class ExchangeManagementTest(IonTestCase):
    """
    Testing client classes of User Registration
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        """
        yield self._start_container()
        services = []
        yield self._spawn_processes(services)
        self.controller = BrokerController()
        yield self.controller.start()
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self.controller.stop()
        # yield self.exchange_registry_client.clear_exchange_registry()
        yield self._stop_container()



    # @defer.inlineCallbacks
    def test_open_close(self):
        """
        A lower level test to make sure all of our resource definitions are 
        actually usable.  Later on, this code will only be used within the
        boilerplate code.
        """


    @defer.inlineCallbacks
    def test_create_exchange(self):
        """
        A lower level test to make sure all of our resource definitions are 
        actually usable.  Later on, this code will only be used within the
        boilerplate code.
        """
        yield self.controller.create_exchange(
                         exchange='brian',
                         type='direct', 
                         passive=False, 
                         durable=False,
                         auto_delete=False, 
                         internal=False, 
                         nowait=False        )
        