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
from ion.services.coi.exchange.exchange_registry import ExchangeRegistryClient

from ion.resources import coi_resource_descriptions

class ExchangeClientTest(IonTestCase):
    """
    Testing client classes of User Registration
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        """
        yield self._start_container()

        services = [{'name':'exchange_registry','module':'ion.services.coi.exchange.exchange_registry','class':'ExchangeRegistryService'}]
        supervisor = yield self._spawn_processes(services)

        self.exchange_registry_client = ExchangeRegistryClient(proc=supervisor)


    @defer.inlineCallbacks
    def tearDown(self):
        # yield self.exchange_registry_client.clear_exchange_registry()
        yield self._stop_container()


    @defer.inlineCallbacks
    def x_test_none(self):
        """
        """

        pass