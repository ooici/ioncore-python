#!/usr/bin/env python

"""
@file ion/play/test/test_data_acquisition.py
@test ion.services.sa.data_acquisition Example unit tests for sample code.
@author Michael Meisinger
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.agents.instrumentagents.instrument_agent import InstrumentAgentClient
from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator
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

        log.info("******* Now testing: Create instrument from UI")
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

        log.info("******* Now testing: Create data product from UI")
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
    Testing instrument management service in end-to-end roundtrip mode
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'instreg','module':'ion.services.coi.agent_registry','class':'AgentRegistryService'},
            {'name':'instreg','module':'ion.services.sa.instrument_registry','class':'InstrumentRegistryService'},
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'},
            {'name':'dprodreg','module':'ion.services.sa.data_product_registry','class':'DataProductRegistryService'},
            {'name':'instmgmt','module':'ion.services.sa.instrument_management','class':'InstrumentManagementService'},

            {'name':'SBE49IA','module':'ion.agents.instrumentagents.SBE49_IA','class':'SBE49InstrumentAgent'},
        ]

        sup = yield self._spawn_processes(services)

        #self.agreg_client = AgentRegistryClient(proc=sup)
        #yield self.agreg_client.clear_registry()

        self.ia_pid = sup.get_child_id('SBE49IA')
        self.iaclient = InstrumentAgentClient(proc=sup, target=self.ia_pid)

        self.imc = InstrumentManagementClient(proc=sup)

        self.newInstrument = {'manufacturer' : "SeaBird Electronics",
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}

        instrument = yield self.imc.create_new_instrument(self.newInstrument)
        self.inst_id = instrument.RegistryIdentity
        log.info("*** Instrument created with ID="+str(self.inst_id))

        self.simulator = Simulator(self.inst_id, 9000)
        self.simulator.start()

        yield self.iaclient.register_resource(self.inst_id)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self.simulator.stop()
        yield Simulator.stop_all_simulators()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_get_status(self):
        """
        Get status back from instrument agent associated with instrument id
        """
        res = yield self.imc.get_instrument_state(self.inst_id)
        self.assertNotEqual(res, None)
        log.info("Instrument status: " +str(res))

    @defer.inlineCallbacks
    def test_execute_command(self):
        """
        Execute command through instrument agent associated with instrument id
        """
        res = yield self.imc.execute_command(self.inst_id, 'start', [1])
        log.info("Command result 1" +str(res))

    @defer.inlineCallbacks
    def test_start_agent(self):
        """
        Start the agent with all
        """
        newInstrument = {'manufacturer' : "SeaBird Electronics",
                 'model' : "SBE49",
                 'serial_num' : "99931",
                 'fw_version' : "1"}

        instrument = yield self.imc.create_new_instrument(newInstrument)
        inst_id1 = instrument.RegistryIdentity
        log.info("*** Instrument 2 created with ID="+str(inst_id1))

        res = yield self.imc.start_instrument_agent(inst_id1, 'SBE49')
        log.info("Command result 1" +str(res))
