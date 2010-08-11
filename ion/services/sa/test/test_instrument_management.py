#!/usr/bin/env python

"""
@file ion/play/test/test_data_acquisition.py
@test ion.services.sa.data_acquisition Example unit tests for sample code.
@author Michael Meisinger
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.services.sa.instrument_management import InstrumentManagementClient
from ion.test.iontest import IonTestCase

class DataAcquisitionTest(IonTestCase):
    """
    Testing instrument management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'instreg','module':'ion.services.sa.instrument_registry','class':'InstrumentRegistryService'},
            {'name':'dprodreg','module':'ion.services.sa.data_product_registry','class':'DataProductRegistryService'},
            {'name':'instmgmt','module':'ion.services.sa.instrument_management','class':'InstrumentManagementService'}
        ]

        sup = yield self._spawn_processes(services)

        self.imc = InstrumentManagementClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_management_service(self):
        """
        Accepts an dictionary containing updates to the instrument registry.
        Updates are made to the registries.
        """

        logging.info("******* Now testing: Create instrument from UI")
        userUpdate = {'manufacturer' : "SeaBird Electronics",
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}
        instrumentID = "500"
        userUpdate['instrumentID'] = instrumentID

        self.instrument = yield self.imc.create_new_instrument(userUpdate)

        self.assertEqual(self.instrument.instrumentID, "500")
        self.assertEqual(self.instrument.manufacturer, "SeaBird Electronics")
        self.assertEqual(self.instrument.model, "unknown model") #change made
        self.assertEqual(self.instrument.serial_num, "1234")
        self.assertEqual(self.instrument.fw_version, "1")

        logging.info("******* Now testing: Create data product from UI")
        dataProductInput = {'dataformat' : "binary"}

        self.dataproduct = yield self.imc.register_data_product(dataProductInput)

        self.assertEqual(self.dataproduct.dataformat, "binary")


    @defer.inlineCallbacks
    def test_direct_access(self):

        """
        Switches direct_access mode to ON in the instrument registry.
        """

        userUpdate = {'direct_access' : "on"}
        self.instrument = yield self.imc.create_new_instrument(userUpdate)
        self.assertEqual(self.instrument.direct_access, "on")
