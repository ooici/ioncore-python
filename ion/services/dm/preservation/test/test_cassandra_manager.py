#!/usr/bin/env python

"""
@file ion/services/dm/preservation/test/test_cassandra_manager_service.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.trial import unittest

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.dm.preservation.preservation_service import PreservationClient, PreservationService

class PreservationTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 30
        services = [
            {'name': 'preservation_registry',
             'module': 'ion.services.dm.preservation.preservation_registry',
             'class':'PreservationRegistryService'},
            {'name': 'preservation_service',
             'module': 'ion.services.dm.preservation.preservation_service',
             'class':'PreservationService'},           
        ]
        sup = yield self._spawn_processes(services)
        self.pc = PreservationClient(proc=sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantiation_only(self):
        pass

    def test_create_archive(self):
        """
        Use fetcher service to create dataset so we can test without
        actual messaging.
        """
        raise unittest.SkipTest('Not implemented')

        
