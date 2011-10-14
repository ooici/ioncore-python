#!/usr/bin/env python

"""
@file ion/services/dm/presentation/test/test_catalog_management.py
@test ion.services.dm.presentation.catalog_management
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import Process
from ion.services.dm.presentation.catalog_management import CatalogManagementServiceClient
from ion.test.iontest import IonTestCase


class CatalogManagementTest(IonTestCase):
    """
    Testing catalog management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'catalogmgmt',
                'module':'ion.services.dm.presentation.catalog_management',
                'class':'CatalogManagementServiceClient'
            }
        ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')
        self.sup = sup

        self.cms = CatalogManagementServiceClient(proc=sup)
        self._proc = Process()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_define_catalog(self):
        """
        Accepts a dictionary containing metadata about a data product.
        Updates are made to the registries.
        """

        log.info("test_define_catalog Now testing: Create a catalog ")

        result = yield self.cms.define_catalog(catalogDefinition='TBD')

        log.info("test_define_catalog Finished testing: Create a catalog resource")