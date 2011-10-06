#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/test/test_instrument_management.py
@test ion.services.sa.instrument_management.instrument_management
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.process.process import Process
from ion.services.sa.instrument_management.instrument_management import InstrumentManagementServiceClient
from ion.test.iontest import IonTestCase


class InstrumentManagementTest(IonTestCase):
    """
    Testing data product management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'instrumentmgmt',
                'module':'ion.services.sa.instrument_management.instrument_management',
                'class':'InstrumentManagementServiceClient'
            }
        ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')
        self.sup = sup

        self.imc = InstrumentManagementServiceClient(proc=sup)
        self._proc = Process()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_define_instrument(self):
        """
        Accepts a dictionary containing metadata about a data product.
        Updates are made to the registries.
        """

        log.info("test_define_instrument Now testing: Create a instrument resource")

        result = yield self.imc.define_instrument(serialNumber='12345', make='Seabird', model='SBE37')

        log.info("test_define_instrument Finished testing: Create a instrument resource")
  