#!/usr/bin/env python

"""
@file ion/services/sa/instrument_management/test/test_instrument_management.py
@test ion.services.sa.instrument_management.instrument_management
@author
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.agents.instrumentagents.instrument_agent as instrument_agent

from ion.core.process.process import Process
from ion.services.sa.instrument_management.instrument_management import InstrumentManagementServiceClient
from twisted.trial.unittest import SkipTest
from ion.test.iontest import IonTestCase

from ion.agents.instrumentagents.simulators.sim_NMEA0183_preplanned \
    import NMEA0183SimPrePlanned as sim
from ion.agents.instrumentagents.simulators.sim_NMEA0183 import SERPORTSLAVE

from ion.agents.instrumentagents.instrument_constants import \
    AgentState, \
    AgentStatus, \
    InstErrorCode, \
    AgentEvent, \
    AgentCommand

"""AgentParameter, \
    AgentConnectionState, driver_client, \
    DriverAnnouncement, DriverParameter, DriverChannel, \
    ObservatoryState, DriverStatus, InstrumentCapability, DriverCapability, \
    MetadataParameter, Datatype, TimeSource, ConnectionMethod, \
    AgentStatus, ObservatoryCapability
"""


class InstrumentManagementTest(IonTestCase):
    """
    Testing data product management service
    """

    @defer.inlineCallbacks
    def setUp(self):

        self._sim = sim()
        yield self._sim.SetupSimulator()
        if self._sim.IsSimOK():
            log.info ('----- Simulator launched.')
        self.assertEqual (self._sim.IsSimulatorRunning(), 1)

        yield self._start_container()



        # Driver and agent configuration. Configuration data will ultimately be accessed via
        # some persistence mechanism: platform filesystem or a device registry.
        # For now, we pass all configuration data that would be read this way as process arguments.
        driver_config       = { 'port':         SERPORTSLAVE,
                                'baudrate':     19200,
                                'bytesize':     8,
                                'parity':       "N",
                                'stopbits':     1,
                                'timeout':      0,
                                'xonxoff':      0,
                                'rtscts':       0 }
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
        spawnargs           = { 'driver-desc':  driver_desc,
                                'client-desc':  driver_client_desc,
                                'driver-config':driver_config,
                                'agent-config': agent_config }

        # Process description for the instrument agent.
        ia_desc             = { 'name':         'instrument_agent_potato',
                                'module':       'ion.agents.instrumentagents.instrument_agent',
                                'class':        'InstrumentAgent',
                                'spawnargs':    spawnargs }


        ims_desc            = {
            'name':'instrumentmgmt',
            'module':'ion.services.sa.instrument_management.instrument_management',
            'class':'InstrumentManagementServiceClient'
            }

        services = [ims_desc, ia_desc]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        supervisor      = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')
        self.sup        = supervisor

        self.imc        = InstrumentManagementServiceClient(proc=self.sup)
        self.ia_svc_id  = yield self.sup.get_child_id("instrument_agent_potato")
        log.debug("ia_svc_id=%s", str(self.ia_svc_id))
        self.iac        = instrument_agent.InstrumentAgentClient(proc=self.sup, target=self.ia_svc_id)
        self._proc      = Process()


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._sim.StopSimulator()
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_define_instrument(self):
        """
        Tests whether an instrument can be added to the DB
        """
        serial = 12345
        yield self._define_instrument(serial)



    @defer.inlineCallbacks
    def test_define_duplicate_instrument(self):
        """
        Tests whether an instrument can be added twice (hopefully not)
        """

        serial = 12345
        result = yield self._define_instrument(serial)

        log.info("Registering the same instrument ... should fail")
        result = yield self.imc.define_instrument(serialNumber=serial, make='Seabird', model='SBE37')
        log.debug(str(result))
        self.failUnlessEqual(False, result["success"])


    @defer.inlineCallbacks
    def test_define_get_instrument(self):
        """
        Tests whether an instrument can be added to the DB and checks valididty
        """

        serial = 12345
        result = yield self._define_instrument(serial)
        inst_resource = result["resourceId"]

        log.info("Saved as resourceId=" + inst_resource + " ... verifying")

        r2 = yield self.imc.get_instrument(resourceId=inst_resource)
        self.failUnlessEqual(True, r2["success"])
        self.assertTrue("instrument" in r2)
        self.assertTrue("serialNumber" in r2["instrument"])
        self.assertTrue("make" in r2["instrument"])
        self.assertTrue("model" in r2["instrument"])
        self.failUnlessEqual(serial, r2["instrument"]["serialNumber"])
        self.failUnlessEqual("Seabird", r2["instrument"]["make"])
        self.failUnlessEqual("SBE37", r2["instrument"]["model"])


    @defer.inlineCallbacks
    def test_define_update_instrument(self):
        """
        Tests whether an instrument can be added to the DB and checks valididty
        """

        serial = 12345
        result = yield self._define_instrument(serial)
        inst_resource = result["resourceId"]

        log.info("Saved as resourceId=" + inst_resource + " ... now updating")

        r2 = yield self.imc.update_instrument(resourceId=inst_resource, serialNumber=55555)

        self.failUnlessEqual(True, r2["success"])

        log.info("Updated, now checking")
        r3 = yield self.imc.get_instrument(resourceId=inst_resource)
        self.assertTrue("instrument" in r3, msg=("r3="+str(r3)))
        self.assertTrue("serialNumber" in r3["instrument"])
        self.assertTrue("make" in r3["instrument"])
        self.assertTrue("model" in r3["instrument"])
        self.failIfEqual(serial, r3["instrument"]["serialNumber"])
        self.failUnlessEqual(55555, r3["instrument"]["serialNumber"])
        self.failUnlessEqual("Seabird", r3["instrument"]["make"])
        self.failUnlessEqual("SBE37", r3["instrument"]["model"])


    @defer.inlineCallbacks
    def test_find_instrument(self):
        log.info("Creating five SBE37 instruments")
        for i in range(34, 39):
            yield self.imc.define_instrument(serialNumber=i, make='Seabird', model='SBE37')

        log.info("Creating one SBE38 instrument")
        yield self.imc.define_instrument(serialNumber=i, make='Seabird', model='SBE38')

        log.info("Finding 1 instrument")
        one = yield self.imc.find_instrument(model='SBE38')
        self.failUnlessEqual(True, one["success"])
        self.assertTrue("instruments" in one, msg=("one=" + str(one)))
        self.failUnlessEqual(len(one["instruments"]), 1)
        for k, v in one["instruments"].iteritems():
            self.failUnlessEqual("SBE38", v["model"])


        log.info("Finding 5 instruments")
        five = yield self.imc.find_instrument(model='SBE37')
        self.assertTrue("instruments" in five, msg=("five=" + str(five)))
        self.failUnlessEqual(len(five["instruments"]), 5)
        for k, v in five["instruments"].iteritems():
            self.failUnlessEqual("SBE37", v["model"])


    @defer.inlineCallbacks
    def test_retire_instrument(self):
        """
        Tests whether an instrument can be added to the DB and checks valididty
        """

        serial = 12345
        result = yield self._define_instrument(serial)
        inst_resource = result["resourceId"]

        log.info("Saved as resourceId=" + inst_resource + " ... now updating")

        r2 = yield self.imc.retire_instrument(resourceId=inst_resource)

        self.failUnlessEqual(True, r2["success"])

        log.info("Retired, now checking")
        r3 = yield self.imc.get_instrument(resourceId=inst_resource)
        self.assertTrue("instrument" in r3, msg=("r3="+str(r3)))
        self.assertTrue("retired" in r3["instrument"])



    @defer.inlineCallbacks
    def test_activate_instrument(self):
        serial = 12345

        result = yield self._define_instrument(serial)
        inst_resource = result["resourceId"]

        log.info("Attempting to activate an instrument with id=" + inst_resource)
        result = yield self.imc.activate_instrument(resourceId=inst_resource, agentId=str(self.ia_svc_id))
        self.failUnlessEqual(True, result["success"])


    @defer.inlineCallbacks
    def test_activate_instrument_nonexistent_agent(self):
        raise SkipTest("to be completed: ensure that the IMS can detect a bogus instrument agent client")


    @defer.inlineCallbacks
    def test_activate_nonexistent_instrument(self):

        log.info("Attempting to activate an instrument not in the db")
        result = yield self.imc.activate_instrument(resourceId="HAHAHA!", agentId="instrument_agent_potato")
        self.failUnlessEqual(False, result["success"])


    @defer.inlineCallbacks
    def test_request_direct_access(self):
        serial = 12345

        result = yield self._define_instrument(serial)
        inst_resource = result["resourceId"]

        log.info("Attempting to activate an instrument with id=" + inst_resource)
        result = yield self.imc.activate_instrument(resourceId=inst_resource, agentId=str(self.ia_svc_id))
        self.failUnlessEqual(True, result["success"])

        log.info("Putting instrument agent into observatory mode")
        log.debug ('----- Begin  IA transaction.')
        reply = yield self.iac.start_transaction()
        success = reply['success']
        tid = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok (success))
        self.assertEqual(type (tid), str)
        self.assertEqual(len (tid), 36)

        # Initialize the agent to bring up the driver and client.
        log.debug ('----- Initialize the agent and driver.')
        cmd = [AgentCommand.TRANSITION, AgentEvent.INITIALIZE]
        reply = yield self.iac.execute_observatory (cmd, tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok (success))

        # Get IA into an active running state
        log.debug ('----- Get IA into an active running state.')
        cmd = [AgentCommand.TRANSITION, AgentEvent.GO_ACTIVE]
        reply = yield self.iac.execute_observatory (cmd, tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok (success))
        cmd = [AgentCommand.TRANSITION, AgentEvent.RUN]
        reply = yield self.iac.execute_observatory (cmd, tid)
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok (success))

        # Verify that the agent is in observatory mode.
        log.debug ('----- Verify that the agent is in observatory mode...')
        params = [AgentStatus.AGENT_STATE]
        reply = yield self.iac.get_observatory_status (params, tid)
        success = reply['success']
        result = reply['result']
        agent_state = result[AgentStatus.AGENT_STATE][1]
        self.assert_(InstErrorCode.is_ok (success))
        log.debug ('           ... agent state now: ' + str (agent_state))
        self.assert_(agent_state == AgentState.OBSERVATORY_MODE)

        # End the transaction.
        log.debug ('----- End the explicit transaction.')
        reply = yield self.iac.end_transaction (tid)

        log.info("Attempting to request direct access")
        reply2 = yield self.imc.request_direct_access(identity="me", resourceId=inst_resource)
        log.debug(reply2)
        self.failUnlessEqual(True, reply2["success"])


    @defer.inlineCallbacks
    def test_request_direct_access_negative(self):
        serial = 12345

        result = yield self._define_instrument(serial)
        inst_resource = result["resourceId"]

        log.info("Un-activated case should fail")
        reply1 = yield self.imc.request_direct_access(identity="me", resourceId=inst_resource)
        log.debug(reply1)
        self.failIfEqual(True, reply1["success"])


        log.info("Attempting to activate an instrument with id=" + inst_resource)
        result = yield self.imc.activate_instrument(resourceId=inst_resource, agentId=str(self.ia_svc_id))
        self.failUnlessEqual(True, result["success"])

        log.info("Un-activated non-observatory mode should fail")
        reply2 = yield self.imc.request_direct_access(identity="me", resourceId=inst_resource)
        log.debug(reply2)
        self.failIfEqual(True, reply2["success"])





    ########################################################
    #
    # PLATFORM TESTS #######################################~



    @defer.inlineCallbacks
    def test_define_platform(self):
        """
        Tests whether a platform can be added to the DB.
        """

        serial = 12345
        result = yield self._define_platform(serial)
        log.info("Success with new platform resourceId=" + str(result["resourceId"]))



    @defer.inlineCallbacks
    def test_define_duplicate_platform(self):
        """
        Tests whether a platform can be added twice (hopefully not)
        """

        serial = 12345
        result = yield self._define_platform(serial)

        log.info("Registering the same platform ... should fail")
        result = yield self.imc.define_platform(serialNumber=serial, name='HokeyPlatform')
        self.failUnlessEqual(False, result["success"])



    @defer.inlineCallbacks
    def test_define_get_platform(self):
        """
        Tests whether a platform can be added to the DB.
        """

        serial = 12345
        result = yield self._define_platform(serial)

        plat_resource = result["resourceId"]
        r2 = yield self.imc.get_platform(resourceId=plat_resource)
        self.failUnlessEqual(True, r2["success"])
        self.assertTrue("platform" in r2)
        self.assertTrue("serialNumber" in r2["platform"])
        self.assertTrue("name" in r2["platform"])
        self.failUnlessEqual(serial, r2["platform"]["serialNumber"])
        self.failUnlessEqual("HokeyPlatform", r2["platform"]["name"])



    @defer.inlineCallbacks
    def test_define_update_platform(self):
        """
        Tests whether an platform can be added to the DB and checks valididty
        """

        serial = 12345
        result = yield self._define_platform(serial)
        inst_resource = result["resourceId"]

        log.info("Saved as resourceId=" + inst_resource + " ... now updating")

        r2 = yield self.imc.update_platform(resourceId=inst_resource, serialNumber=55555)

        self.failUnlessEqual(True, r2["success"])

        log.info("Updated, now checking")
        r3 = yield self.imc.get_platform(resourceId=inst_resource)
        self.assertTrue("platform" in r3, msg=("r3="+str(r3)))
        self.assertTrue("serialNumber" in r3["platform"])
        self.assertTrue("name" in r3["platform"])
        self.failIfEqual(serial, r3["platform"]["serialNumber"])
        self.failUnlessEqual(55555, r3["platform"]["serialNumber"])
        self.failUnlessEqual("HokeyPlatform", r3["platform"]["name"])


    @defer.inlineCallbacks
    def test_find_platform(self):
        log.info("Creating five Alpha platforms")
        for i in range(34, 39):
            yield self.imc.define_platform(serialNumber=i, name='Alpha')

        log.info("Creating one Beta platform")
        yield self.imc.define_platform(serialNumber=i, name='Beta')

        log.info("Finding 1 platform")
        one = yield self.imc.find_platform(name='Beta')
        self.failUnlessEqual(True, one["success"])
        self.assertTrue("platforms" in one, msg=("one=" + str(one)))
        self.failUnlessEqual(len(one["platforms"]), 1)
        log.debug(one["platforms"])
        for k, v in one["platforms"].iteritems():
            self.failUnlessEqual("Beta", v["name"])


        log.info("Finding 5 platforms")
        five = yield self.imc.find_platform(name='Alpha')
        self.assertTrue("platforms" in five, msg=("five=" + str(five)))
        self.failUnlessEqual(len(five["platforms"]), 5)
        for k, v in five["platforms"].iteritems():
            self.failUnlessEqual("Alpha", v["name"])


    @defer.inlineCallbacks
    def test_retire_platform(self):
        """
        Tests whether an platform can be added to the DB and checks valididty
        """

        serial = 12345
        result = yield self._define_platform(serial)
        inst_resource = result["resourceId"]

        log.info("Saved as resourceId=" + inst_resource + " ... now updating")

        r2 = yield self.imc.retire_platform(resourceId=inst_resource)

        self.failUnlessEqual(True, r2["success"])

        log.info("Retired, now checking")
        r3 = yield self.imc.get_platform(resourceId=inst_resource)
        self.assertTrue("platform" in r3, msg=("r3="+str(r3)))
        self.assertTrue("retired" in r3["platform"])






    @defer.inlineCallbacks
    def _define_instrument(self, serial):
        log.info("Attempting to register 'Seabird' 'SBE37' with serial '" + str(serial) + "'")
        result = yield self.imc.define_instrument(serialNumber=serial, make='Seabird', model='SBE37')
        log.debug(str(result))
        self.failUnlessEqual(True, result["success"])

        defer.returnValue(result)

    @defer.inlineCallbacks
    def _define_platform(self, serial):

        log.info("Attempting to register 'HokeyPlatform' with serial '" + str(serial) + "'")
        result = yield self.imc.define_platform(serialNumber=serial, name='HokeyPlatform')
        log.debug(str(result))
        self.failUnlessEqual(True, result["success"], str(result))
        log.info("Success with new platform resourceId=" + str(result["resourceId"]))

        defer.returnValue(result)



