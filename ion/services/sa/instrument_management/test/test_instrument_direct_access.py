#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/test/test_instrument_direct_access.py
@test ion.services.sa.instrument_management.instrument_direct_access
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
import ion.services.sa.instrument_management.instrument_direct_access as direct_access
from ion.agents.instrumentagents.instrument_constants import AgentCommand
from ion.agents.instrumentagents.instrument_constants import AgentEvent
from ion.agents.instrumentagents.instrument_constants import AgentStatus
from ion.agents.instrumentagents.instrument_constants import AgentState
from ion.agents.instrumentagents.instrument_constants import InstErrorCode
from ion.core.process.process import Process
#from ion.services.sa.instrument_management.instrument_direct_access import InstrumentDirectAccessServiceClient
from ion.test.iontest import IonTestCase
import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.agents.instrumentagents.simulators.sim_NMEA0183_preplanned \
    import NMEA0183SimPrePlanned as sim
from ion.agents.instrumentagents.simulators.sim_NMEA0183 \
    import SERPORTSLAVE

import ion.util.procutils as pu

INST_NAME = "gpssim"

class InstrumentDirectAccessTest(IonTestCase):
    """
    Testing data product management service
    """

    timeout = 120

    @defer.inlineCallbacks
    def setUp(self):
        """  """
        log.info("START: InstrumentDirectAccessTest.setUp()")

        log.debug ('----- Launching simulated instrument...')
        self._sim = sim()
        yield self._sim.SetupSimulator()
        if self._sim.IsSimOK():
            log.info ('           ... simulator launched successfully.')
        self.assertEqual (self._sim.IsSimulatorRunning(), 1)

        log.debug ('----- Starting container')
        yield self._start_container()

        # Prepare arguments for launching the instrument agent and direct access services
        IA_spawnargs = self.PrepareInstrumentAgentSpawnArgs()
        DA_args = {'name':      'instrument_direct_access',
                   'module':    'ion.services.sa.instrument_management.instrument_direct_access',
                   'class':     'InstrumentDirectAccessServiceClient'}
        IA_args = {'name':      INST_NAME,
                   'module':    'ion.agents.instrumentagents.instrument_agent',
                   'class':     'InstrumentAgent',
                   'spawnargs': IA_spawnargs }
        services = [IA_args, DA_args]

        self._proc = Process()
        log.debug ('----- Spawning services')
        self.sup = yield self._spawn_processes (services)
        log.debug ('----- Spawning service clients')
        self.ia_id = yield self.sup.get_child_id (INST_NAME)
        self.ia_client = instrument_agent.InstrumentAgentClient (proc = self.sup, target = self.ia_id)
        self.da_id = yield self.sup.get_child_id ('instrument_direct_access')
        self.da_client = direct_access.InstrumentDirectAccessServiceClient (proc = self.sup, target = self.da_id)
        log.debug ("test case id: " + str (self._proc.id))
        log.debug ("ia_id:        " + str (self.ia_id))
        log.debug ("ia_client id: " + str (self.ia_client.proc.id))
        log.debug ("da_id:        " + str (self.da_id))
        log.debug ("da_client id: " + str (self.da_client.proc.id))

        log.info("FINISH: InstrumentDirectAccessTest.setUp()")

    def PrepareInstrumentAgentSpawnArgs (self):
        """ Driver and agent configuration. Configuration data will ultimately be accessed via
            some persistence mechanism: platform filesystem or a device registry.
            For now, we pass all configuration data that would be read this way as process arguments. """
        device_port         = SERPORTSLAVE
        device_baud         = 19200
        device_bytesize     = 8
        device_parity       = 'N'
        device_stopbits     = 1
        device_timeout      = 0
        device_xonxoff      = 0
        device_rtscts       = 0
        driver_config       = { 'port':         device_port,
                                'baudrate':     device_baud,
                                'bytesize':     device_bytesize,
                                'parity':       device_parity,
                                'stopbits':     device_stopbits,
                                'timeout':      device_timeout,
                                'xonxoff':      device_xonxoff,
                                'rtscts':       device_rtscts }
        agent_config        = {}

        # Process description for the instrument driver.
        driver_desc         = { 'name':         'NMEA0183_Driver',
                                'module':       'ion.agents.instrumentagents.driver_NMEA0183',
                                'class':        'NMEADeviceDriver',
                                'spawnargs':  { 'config': driver_config } }

        # Process description for the instrument driver client.
        driver_client_desc  = { 'name':         'NMEA0813_Client',
                                'module':       'ion.agents.instrumentagents.driver_NMEA0183',
                                'class':        'NMEADeviceDriverClient',
                                'spawnargs':    {} }

        # Spawnargs for the instrument agent.
        return                { 'driver-desc':  driver_desc,
                                'client-desc':  driver_client_desc,
                                'driver-config':driver_config,
                                'agent-config': agent_config }

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._sim.StopSimulator()
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_Set_Direct_State(self):
        """
        Places the instrument into Direct Access state.
        """
        log.info("TEST START: test_Set_Direct_State()")

        # Begin an explicit transaction.
        log.debug ('----- Begin an explicit transaction.')
        reply = yield self.ia_client.start_transaction()
        success = reply['success']
        tid = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok (success))
        self.assertEqual(type (tid), str)
        self.assertEqual(len (tid), 36)

        # Initialize the agent to bring up the driver and client.
        log.debug ('----- Initialize the agent and driver.')
        cmd = [AgentCommand.TRANSITION, AgentEvent.INITIALIZE]
        reply = yield self.ia_client.execute_observatory (cmd, tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok (success))

        # Get IA into an active running state
        log.debug ('----- Get IA into an active running state.')
        cmd = [AgentCommand.TRANSITION, AgentEvent.GO_ACTIVE]
        reply = yield self.ia_client.execute_observatory (cmd, tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok (success))
        cmd = [AgentCommand.TRANSITION, AgentEvent.RUN]
        reply = yield self.ia_client.execute_observatory (cmd, tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok (success))

        # Verify that the agent is in observatory mode.
        log.debug ('----- Verify that the agent is in observatory mode...')
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.ia_client.get_observatory_status (params, tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok (success))
        log.debug ('           ... agent state now: ' + str (agent_state))
        self.assert_(agent_state == AgentState.OBSERVATORY_MODE)

        # End the transaction.
        log.debug ('----- End the explicit transaction.')
        reply = yield self.ia_client.end_transaction (tid)
        success = reply['success']
        self.assert_ (InstErrorCode.is_ok (success))

        yield pu.asleep(3)
        result = yield self.da_client.start_session (instrumentAgent=INST_NAME)

        log.info ("TEST FINISH: test_Set_Direct_State()")
