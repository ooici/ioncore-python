#!/usr/bin/env python

"""
@file ion/integration/sa/test/test_instrument_integration_service.py
@test ion.services.sa.instrument_integration_service Example unit tests for sample code.
@author Maurice Manning
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

INSTRUMENTDATA_EVENT_ID = 5001

from ion.integration.sa.instrument_integration_service import InstrumentIntegrationClient
from ion.test.iontest import IonTestCase
from ion.services.coi.resource_registry.resource_registry import ResourceRegistryClient, ResourceRegistryError
from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceInstance, RESOURCE_TYPE
from ion.services.dm.distribution.events import DataEventPublisher
from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_RESOURCE_TYPES, ION_IDENTITIES, ID_CFG, PRELOAD_CFG, ION_DATASETS_CFG, ION_DATASETS, NAME_CFG, DEFAULT_RESOURCE_TYPE_ID
from ion.agents.instrumentagents.instrument_constants import DriverChannel
from ion.services.dm.distribution.events import DataEventSubscriber, DataBlockEventSubscriber

from ion.core.messaging.message_client import MessageClient

from ion.core.object import object_utils

AIS_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9001, version=1)
AIS_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9002, version=1)
AIS_RESPONSE_ERROR_TYPE = object_utils.create_type_identifier(object_id=9003, version=1)
CREATE_INSTRUMENT_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9301, version=1)
CREATE_INSTRUMENT_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9302, version=1)
START_INSTRUMENT_AGENT_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9303, version=1)
START_INSTRUMENT_AGENT_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9304, version=1)
START_INSTRUMENT_SAMPLING_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9305, version=1)
START_INSTRUMENT_SAMPLING_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9306, version=1)
STOP_INSTRUMENT_SAMPLING_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9307, version=1)
STOP_INSTRUMENT_SAMPLING_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9308, version=1)
GET_INSTRUMENT_STATE_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9309, version=1)
GET_INSTRUMENT_STATE_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9310, version=1)
SET_INSTRUMENT_STATE_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9311, version=1)
SET_INSTRUMENT_STATE_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9312, version=1)
GET_INSTRUMENT_LIST_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9313, version=1)
GET_INSTRUMENT_LIST_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9314, version=1)

from ion.agents.instrumentagents.simulators.sim_NMEA0183_preplanned \
    import NMEA0183SimPrePlanned as sim

import ion.util.procutils as pu


class InstrumentIntegrationServiceTest(IonTestCase):
    """
    Testing instrument integration service
    """

    timeout = 300

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        self._sim = sim()
        self.assertTrue (self._sim.IsSimulatorRunning())
        
        services = [
            {
                'name':'instrument_integration_service',
                'module':'ion.integration.sa.instrument_integration_service',
                'class':'InstrumentIntegrationService'
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True}}

            },
            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                'spawnargs':{'datastore_service':'datastore'}
            },
        ]

        sup = yield self._spawn_processes(services)
        self.sup = sup

        self.rrc = ResourceRegistryClient(proc=sup)
        self.rc = ResourceClient(proc=sup)
        self.iic = InstrumentIntegrationClient(proc=sup)
        self.user_id = 0

    @defer.inlineCallbacks
    def tearDown(self):
        #yield self._shutdown_processes()
        self._sim.StopSimulator()
        yield self._stop_container()

    @defer.inlineCallbacks
    def handle_update_event(self, content):
            log.info('IIServiceTest.handle_update_event notification event received ')
            #Check that the item is in the store
            log.info('IIServiceTest.handle_update_event content   : %s', content)
            msg = content['content']
            log.info('IIServiceTest.handle_update_event content dict : %s', msg)

    @defer.inlineCallbacks
    def test_createInstrument(self):
        """
        Create an instrument, create associated instrument agent and get the list of instruments
        """
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Create instrument request')
        msg.message_parameters_reference = msg.CreateObject(CREATE_INSTRUMENT_REQUEST_MSG_TYPE)

        msg.message_parameters_reference.manufacturer = 'NMEA0183'
        msg.message_parameters_reference.model = 'NMEA0183'
        msg.message_parameters_reference.serial_num = '1'
        msg.message_parameters_reference.fw_version = '1'
        rspMsg = yield self.iic.createNewInstrument(msg)
        
        instrument_id = rspMsg.message_parameters_reference[0].instrument_resource_id
        log.info("IIServiceTest test_createInstrument  instrument id: %s ", instrument_id )

        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Start instrument agent request')
        msg.message_parameters_reference = msg.CreateObject(START_INSTRUMENT_AGENT_REQUEST_MSG_TYPE)
        msg.message_parameters_reference.name = 'NMEA0183'
        msg.message_parameters_reference.model = 'NMEA0183'
        msg.message_parameters_reference.instrument_resource_id = instrument_id

        rspMsg = yield self.iic.startInstrumentAgent(msg)
        
        instrument_agent_process_id = rspMsg.message_parameters_reference[0].instrument_agent_resource_id
        instrument_agent_resource_id = rspMsg.message_parameters_reference[0].instrument_agent_process_id
        log.info("IIServiceTest test_create_instrument  instrument agent resource id: %s   process id: %s", instrument_agent_resource_id, instrument_agent_process_id )

        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Instrument list request')
        msg.message_parameters_reference = msg.CreateObject(GET_INSTRUMENT_LIST_REQUEST_MSG_TYPE)

        rspMsg = yield self.iic.getInstrumentList(msg)
        log.info("IIServiceTest test_createInstrument  instrument list: %s ", str(rspMsg.message_parameters_reference[0].instrument_metadata[0]))

        if len(rspMsg.message_parameters_reference[0].instrument_metadata) != 1:
            self.fail('InstrumentIntegrationServiceTest: test_createInstrument returned incorrect number of instruments after create.')

    @defer.inlineCallbacks
    def test_createAndSetInstrument(self):
        """
        Create an instrument, create associated instrument agent then get and set the status
        """
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Create instrument request')
        msg.message_parameters_reference = msg.CreateObject(CREATE_INSTRUMENT_REQUEST_MSG_TYPE)

        msg.message_parameters_reference.manufacturer = 'NMEA0183'
        msg.message_parameters_reference.model = 'NMEA0183'
        msg.message_parameters_reference.serial_num = '1'
        msg.message_parameters_reference.fw_version = '1'

        rspMsg = yield self.iic.createNewInstrument(msg)
        
        instrument_id = rspMsg.message_parameters_reference[0].instrument_resource_id
        log.info("IIServiceTest test_createInstrument  instrument id: %s ", instrument_id )

        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Start instrument agent request')
        msg.message_parameters_reference = msg.CreateObject(START_INSTRUMENT_AGENT_REQUEST_MSG_TYPE)
        #msg.message_parameters_reference.name = 'SeaBird Electronics'
        #msg.message_parameters_reference.model = 'SBE37'
        msg.message_parameters_reference.name = 'NMEA0183'
        msg.message_parameters_reference.model = 'NMEA0183'
        msg.message_parameters_reference.instrument_resource_id = instrument_id

        rspMsg = yield self.iic.startInstrumentAgent(msg)
        
        instrument_agent_process_id = rspMsg.message_parameters_reference[0].instrument_agent_resource_id
        instrument_agent_resource_id = rspMsg.message_parameters_reference[0].instrument_agent_process_id
        log.info("IIServiceTest test_create_instrument  instrument agent resource id: %s   process id: %s", instrument_agent_resource_id, instrument_agent_process_id )

        stateReqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Get instrument state request')
        stateReqMsg.message_parameters_reference = msg.CreateObject(GET_INSTRUMENT_STATE_REQUEST_MSG_TYPE)
        stateReqMsg.message_parameters_reference.instrument_resource_id = instrument_id

        rspMsg = yield self.iic.getInstrumentState(stateReqMsg)
        properties = rspMsg.message_parameters_reference[0].properties

        log.info("IIServiceTest test_createInstrument  instrument state: %s ", str(properties))

        # Set a few parameters. This will test the device set functions
        # and set up the driver for sampling commands.
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Set instrument state request')
        msg.message_parameters_reference = msg.CreateObject(SET_INSTRUMENT_STATE_REQUEST_MSG_TYPE)
        msg.message_parameters_reference.instrument_resource_id = instrument_id
        msg.message_parameters_reference.properties.alt_msl = 10.6
        msg.message_parameters_reference.properties.earth_datum = 20

        rspMsg = yield self.iic.setInstrumentState(msg)
        log.info("IIServiceTest test_createAndSetInstrument  instrument status: %s ", rspMsg.message_parameters_reference[0].status)

        # Verify the set changes were made.
        rspMsg = yield self.iic.getInstrumentState(stateReqMsg)
        alt_mslReturned = rspMsg.message_parameters_reference[0].properties.alt_msl
        earth_datumReturned = rspMsg.message_parameters_reference[0].properties.earth_datum
        log.info("IIServiceTest test_createAndSetInstrument  INSTRUMENT ALT_MSL 2: %s ", alt_mslReturned)

        self.assertAlmostEqual(alt_mslReturned, 10.6, 4)
        self.assertEqual(earth_datumReturned, 20)

        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Set instrument state request')
        msg.message_parameters_reference = msg.CreateObject(SET_INSTRUMENT_STATE_REQUEST_MSG_TYPE)
        msg.message_parameters_reference.instrument_resource_id = instrument_id
        msg.message_parameters_reference.properties.alt_msl = 11.6

        rspMsg = yield self.iic.setInstrumentState(msg)

        # Verify the set changes were made.
        rspMsg = yield self.iic.getInstrumentState(stateReqMsg)
        alt_mslReturned = rspMsg.message_parameters_reference[0].properties.alt_msl
        log.info("IIServiceTest test_createAndSetInstrument  INSTRUMENT ALT_MSL 3: %s ", alt_mslReturned)

        self.assertAlmostEqual(alt_mslReturned, 11.6, 4)


    @defer.inlineCallbacks
    def test_createInstrumentStartSampling(self):
        """
        Create an instrument, create associated instrument agent, start sampling and catch the events
        """
        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Create instrument request')
        msg.message_parameters_reference = msg.CreateObject(CREATE_INSTRUMENT_REQUEST_MSG_TYPE)

        msg.message_parameters_reference.manufacturer = 'NMEA0183'
        msg.message_parameters_reference.model = 'NMEA0183'
        msg.message_parameters_reference.serial_num = '1'
        msg.message_parameters_reference.fw_version = '1'

        rspMsg = yield self.iic.createNewInstrument(msg)
        
        instrument_id = rspMsg.message_parameters_reference[0].instrument_resource_id
        log.info("IIServiceTest test_createInstrument  instrument id: %s ", instrument_id )

        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Start instrument agent request')
        msg.message_parameters_reference = msg.CreateObject(START_INSTRUMENT_AGENT_REQUEST_MSG_TYPE)
        msg.message_parameters_reference.name = 'NMEA0183'
        msg.message_parameters_reference.model = 'NMEA0183'
        msg.message_parameters_reference.instrument_resource_id = instrument_id

        rspMsg = yield self.iic.startInstrumentAgent(msg)
        
        instrument_agent_process_id = rspMsg.message_parameters_reference[0].instrument_agent_resource_id
        instrument_agent_resource_id = rspMsg.message_parameters_reference[0].instrument_agent_process_id
        log.info("IIServiceTest test_create_instrument  instrument agent resource id: %s   process id: %s", instrument_agent_resource_id, instrument_agent_process_id )

        #self.sub = DataEventSubscriber(process=self.sup, origin=instrument_agent_process_id)  DataBlockEventSubscriber
        #self.sub = DataBlockEventSubscriber(process=self.sup, origin=instrument_agent_process_id)
        #log.info('IIServiceTest test_createInstrumentStartSampling  set handler for DataEventSubscriber')
        #self.sub.ondata = self.handle_update_event    # need to do something with the data when it is received
        #yield self.sub.register()
        #yield self.sub.initialize()
        #yield self.sub.activate()
        log.info('IIServiceTest test_createInstrumentStartSampling DatasetSupplementAddedEvent activation complete')

        # Start autosampling.
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Start instrument sampling request')
        msg.message_parameters_reference = msg.CreateObject(START_INSTRUMENT_SAMPLING_REQUEST_MSG_TYPE)
        msg.message_parameters_reference.instrument_resource_id = instrument_id

        rspMsg = yield self.iic.startAutoSampling(msg)
        log.info("IIServiceTest test_createInstrumentStartSampling  startAutoSampling: %s ", rspMsg.message_parameters_reference[0].status)

        # Wait for a few samples to arrive.
        yield pu.asleep(10)

        # Stop autosampling.
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='Stop instrument sampling request')
        msg.message_parameters_reference = msg.CreateObject(STOP_INSTRUMENT_SAMPLING_REQUEST_MSG_TYPE)
        msg.message_parameters_reference.instrument_resource_id = instrument_id

        rspMsg = yield self.iic.stopAutoSampling(msg)
        log.info("IIServiceTest test_createInstrumentStartSampling  Stop autosampling: %s ", rspMsg.message_parameters_reference[0].status)

