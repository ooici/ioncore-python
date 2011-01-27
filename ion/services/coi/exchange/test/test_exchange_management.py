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
from ion.services.coi.exchange.exchange_management import ExchangeManagementClient

from ion.resources import coi_resource_descriptions

class ExchangeManagementTest(IonTestCase):
    """
    Testing client classes of User Registration
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        """
        yield self._start_container()
        services = [
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry_beta.resource_registry',
                'class':'ResourceRegistryService',
                'spawnargs':{'datastore_service':'datastore'}
            },
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
                'spawnargs':{'datastore_sevice':'datastore'}
            }
        ]
        sup = yield self._spawn_processes(services)
        self.emc = ExchangeManagementClient(proc=self.test_sup)
        self.sup = sup
        

    @defer.inlineCallbacks
    def tearDown(self):
        # yield self.exchange_registry_client.clear_exchange_registry()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_create_methods(self):
        """
        """
        yield self.emc.create_exchangespace("name","description")
        yield self.emc.create_exchangename("name")

        
