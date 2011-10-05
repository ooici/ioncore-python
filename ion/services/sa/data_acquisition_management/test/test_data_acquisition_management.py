#!/usr/bin/env python

"""
@file ion/services/sa/data_acquisition_management/test/test_data_acquisition_management.py
@test ion.services.sa.data_acquisition_management.data_acquisition_management
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import Process
from ion.services.sa.data_acquisition_management.data_acquisition_management import DataAcquisitionManagementServiceClient
from ion.test.iontest import IonTestCase


class DataAcquisitionManagementTest(IonTestCase):
    """
    Testing data product management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'dataacquisitionmgmt',
                'module':'ion.services.sa.data_acquisition_management.data_acquisition_management',
                'class':'DataAcquisitionManagementServiceClient'
            }
        ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')
        self.sup = sup

        self.damc = DataAcquisitionManagementServiceClient(proc=sup)
        self._proc = Process()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_define_data_agent(self):
        """
        Accepts a dictionary containing metadata about a data product.
        Updates are made to the registries.
        """

        log.info("test_define_data_agent Now testing: Create a data agent")

        result = yield self.damc.define_data_agent(agent='dataAgent')

        log.info("define_data_agent Finished testing: Create a data agent")
  