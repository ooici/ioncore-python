#!/usr/bin/env python

"""
@file ion/services/sa/test/test_data_process_management.py
@test ion.services.sa.data_process_management
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import Process
from ion.services.sa.data_process_management.data_process_management import DataProcessManagementServiceClient
from ion.test.iontest import IonTestCase


class DataProcessManagementTest(IonTestCase):
    """
    Testing data process management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'dataprocessmgmt',
                'module':'ion.services.sa.data_process_management.data_process_management',
                'class':'DataProcessManagementServiceClient'
            }
        ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')
        self.sup = sup

        self.dpmc = DataProcessManagementServiceClient(proc=sup)
        self._proc = Process()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_define_data_process(self):
        """
        Accepts a dictionary containing metadata about a data processes.
        Updates are made to the registries.
        """

        log.info("test_define_data_process Now testing: Create sample data process")

        result = yield self.dpmc.define_data_process(process='proc1')

        log.info("define_data_process Finished testing: Create sample data process")
  