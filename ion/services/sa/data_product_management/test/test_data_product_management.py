#!/usr/bin/env python

"""
@file ion/services/sa/test/test_data_product_management.py
@test ion.services.sa.data_product_management
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import Process
from ion.services.sa.data_product_management.data_product_management import DataProductManagementServiceClient
from ion.test.iontest import IonTestCase


class DataProductManagementTest(IonTestCase):
    """
    Testing data product management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'dataprodmgmt',
                'module':'ion.services.sa.data_product_management.data_product_management',
                'class':'DataProductManagementServiceClient'
            }
        ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')
        self.sup = sup

        self.dpmc = DataProductManagementServiceClient(proc=sup)
        self._proc = Process()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_define_data_product(self):
        """
        Accepts a dictionary containing metadata about a data product.
        Updates are made to the registries.
        """

        log.info("test_define_data_product Now testing: Create sample data product")

        result = yield self.dpmc.define_data_product(title='CTD data', summary='Data from Seabird instrument', keywords='salinity, temperature')

        log.info("define_data_product Finished testing: Create sample data product")




  
  