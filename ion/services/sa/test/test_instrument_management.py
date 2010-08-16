#!/usr/bin/env python

"""
@file ion/play/test/test_data_acquisition.py
@test ion.services.sa.data_acquisition Example unit tests for sample code.
@author Michael Meisinger
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.agents.instrumentagents.test import test_SBE49
from ion.services.coi.agent_registry import AgentRegistryClient
from ion.services.sa.instrument_management import InstrumentManagementClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class InstrumentManagementTest(IonTestCase):
    """
    Testing instrument management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'instreg','module':'ion.services.coi.agent_registry','class':'AgentRegistryService'},
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
    def test_create_instrument(self):
        """
        Accepts an dictionary containing updates to the instrument registry.
        Updates are made to the registries.
        """

        logging.info("******* Now testing: Create instrument from UI")
        userUpdate = {'manufacturer' : "SeaBird Electronics",
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}

        instrument = yield self.imc.create_new_instrument(userUpdate)

        self.assertEqual(instrument.manufacturer, "SeaBird Electronics")
        self.assertEqual(instrument.model, "unknown model") #change made
        self.assertEqual(instrument.serial_num, "1234")
        self.assertEqual(instrument.fw_version, "1")

        instrument_ref = instrument.reference(head=True)
        instrument_id = instrument_ref.RegistryIdentity
        self.assertTrue(instrument_id)

        logging.info("******* Now testing: Create data product from UI")
        dataProductInput = {'dataformat' : "binary",
                            'instrumentID' : instrument_id}

        dataproduct = yield self.imc.create_new_data_product(dataProductInput)

        self.assertEqual(dataproduct.dataformat, "binary")
        self.assertEqual(dataproduct.instrument_ref.RegistryIdentity, instrument_id)


    #@defer.inlineCallbacks
    def xtest_direct_access(self):
        """
        Switches direct_access mode to ON in the instrument registry.
        """

class TestInstMgmtRT(IonTestCase):
    """
    Testing instrument management service
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'instreg','module':'ion.services.coi.agent_registry','class':'AgentRegistryService'},
            {'name':'instreg','module':'ion.services.sa.instrument_registry','class':'InstrumentRegistryService'},
            {'name':'dprodreg','module':'ion.services.sa.data_product_registry','class':'DataProductRegistryService'},
            {'name':'instmgmt','module':'ion.services.sa.instrument_management','class':'InstrumentManagementService'},

            {'name':'SBE49IA','module':'ion.agents.instrumentagents.SBE49_IA','class':'SBE49InstrumentAgent'},
        ]

        sup = yield self._spawn_processes(services)
        self.ia_pid = yield sup.get_child_id('SBE49IA')

        self.agreg_client = AgentRegistryClient(proc=sup)
        yield self.agreg_client.clear_registry()

        self.imc = InstrumentManagementClient(proc=sup)

        self.ia_pid = sup.get_child_id('SBE49IA')
        self.iaclient = InstrumentAgentClient(proc=sup, target=self.ia_pid)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_get_status(self):
        """
        .
        """
        userUpdate = {'manufacturer' : "SeaBird Electronics",
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}

        instrument = yield self.imc.create_new_instrument(userUpdate)
        inst_id = instrument.RegistryIdentity
        logging.info("*** Instrument created with ID="+str(inst_id))

        yield self.iaclient.register_resource(inst_id)

        res = yield self.imc.get_instrument_state(inst_id)
        self.assertNotEqual(res, None)
        logging.info("Instrument status: " +str(res))

    @defer.inlineCallbacks
    def test_execute_command(self):
        """
        .
        """
        self.simproc = test_SBE49.start_SBE49_simulator()
        yield pu.asleep(1)

        try:
            userUpdate = {'manufacturer' : "SeaBird Electronics",
                     'model' : "unknown model",
                     'serial_num' : "1234",
                     'fw_version' : "1"}

            instrument = yield self.imc.create_new_instrument(userUpdate)
            inst_id = instrument.RegistryIdentity
            logging.info("*** Instrument created with ID="+str(inst_id))

            yield self.iaclient.register_resource(inst_id)

            res = yield self.imc.execute_command(inst_id, 'start', [1])
            logging.info("Command result 1" +str(res))

            #
            #command = ['start','now', 1]
            #cmdlist = [command,]
            #cmdres1 = yield self.iaclient.execute_instrument(cmdlist)
            #logging.info("Command result 1" +str(cmdres1))
            #
            #command = ['stop']
            #cmdlist = [command,]
            #cmdres1 = yield self.iaclient.execute_instrument(cmdlist)
            #logging.info("Command result 1" +str(cmdres1))

        finally:
            try:
                yield self._shutdown_processes(self._get_procinstance(self.ia_pid))
            finally:
                test_SBE49.stop_SBE49_simulator(self.simproc)
