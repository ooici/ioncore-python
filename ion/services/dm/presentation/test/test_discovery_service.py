#!/usr/bin/env python

"""
@file ion/services/dm/presentation/test/test_discovery.py
@test ion.services.sdm.presentation.discovery
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import Process
from ion.services.dm.presentation.discovery_service import DiscoveryServiceClient
from ion.test.iontest import IonTestCase


class DiscoveryTest(IonTestCase):
    """
    Testing discovery service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'discovery',
                'module':'ion.services.dm.presentation.discovery_service',
                'class':'DiscoveryServiceClient'
            }
        ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')
        self.sup = sup

        self.ds = DiscoveryServiceClient(proc=sup)
        self._proc = Process()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_find_by_metadata(self):
        """
        Accepts a dictionary containing metadata about a data product.
        Updates are made to the registries.
        """

        log.info("test_find_by_metadata Now testing: search for a resource")

        result = yield self.ds.find_by_metadata(resourceTypes='IdentityResource', keyValueFilters='key=value')

        log.info("test_find_by_metadata Finished testing")
  