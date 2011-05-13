#!/usr/bin/env python

"""
@file ion/integration/sa/test/test_instrument_integration_service.py
@test ion.services.sa.instrument_integration_service Example unit tests for sample code.
@author Maurice Manning
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

INSTRUMENTDATA_EVENT_ID = 5001

from ion.integration.sa.instrument_integration_service import InstrumentIntegrationClient
from ion.test.iontest import IonTestCase
from ion.services.coi.resource_registry.resource_registry import ResourceRegistryClient, ResourceRegistryError
from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceInstance, RESOURCE_TYPE
from ion.services.dm.distribution.events import DataEventPublisher
from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_RESOURCE_TYPES, ION_IDENTITIES, ID_CFG, PRELOAD_CFG, ION_DATASETS_CFG, ION_DATASETS, NAME_CFG, DEFAULT_RESOURCE_TYPE_ID
from ion.agents.instrumentagents.instrument_constants import DriverChannel
from ion.services.dm.distribution.events import DataEventSubscriber

import ion.util.procutils as pu


class InstrumentDataEventPublisher(DataEventPublisher):
    """
    Event Notification Publisher for Subscription Modifications.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = INSTRUMENTDATA_EVENT_ID




class InstrumentIntegrationServiceTest(IonTestCase):
    """
    Testing instrument integration service
    """

    timeout = 300

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'instrumentIntegrationService',
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
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def handle_update_event(self, content):
            log.info('IIServiceTest.handle_update_event notification event received !!!!!!!!!!!!')
            #Check that the item is in the store
            log.info('IIServiceTest.handle_update_event content   : %s', content)
            msg = content['content']
            log.info('IIServiceTest.handle_update_event content dict : %s', msg['additional'])

    @defer.inlineCallbacks
    def test_createInstrument(self):
        """
        Accepts an dictionary containing updates to the instrument registry.
        Updates are made to the registries.
        """

        log.info("IIServiceTest test_createInstrument Now testing: Create instrument from UI")
        userUpdate = {'manufacturer' : "SeaBird Electronics",
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}

        result = yield self.iic.createNewInstrument(userUpdate)
        instrument_id = result['instrument_id']
        log.info("IIServiceTest test_createInstrument  instrument id: %s ", result['instrument_id'] )

        result = yield self.iic.startInstrumentAgent("SeaBird Electronics", instrument_id, "SBE37")
        instrument_agent_process_id = result['instrument_agent_process_id']
        instrument_agent_resource_id = result['instrument_agent_resource_id']
        log.info("IIServiceTest test_create_instrument  instrument agent resource id: %s   process id: %s", instrument_agent_resource_id, instrument_agent_process_id )

        result = yield self.iic.getInstrumentList()
        log.info("IIServiceTest test_createInstrument  instrument list: %s ", result['result'] )


        """
        result = yield self.iic.getInstrumentState(instrument_id)
        log.info("IIServiceTest test_createInstrument  instrument state: %s ", result['result'] )
        resultDict = result['result']
        log.info("IIServiceTest test_createInstrument  INSTRUMENT INTERVAL: %s ",resultDict[(DriverChannel.INSTRUMENT,'INTERVAL')] )

        # Strip off individual success vals to create a set params to
        # restore original config later.
        orig_config = result['result']

        # Set a few parameters. This will test the device set functions
        # and set up the driver for sampling commands.
        params = {}
        params[(DriverChannel.INSTRUMENT,'NAVG')] = 1
        params[(DriverChannel.INSTRUMENT,'INTERVAL')] = 5

        result = yield self.iic.setInstrumentState(instrument_id, params)
        log.info("IIServiceTest test_createInstrument  instrument state: %s ", result['result'] )


        # Verify the set changes were made.
        result = yield self.iic.getInstrumentState(instrument_id)
        log.info("IIServiceTest test_createInstrument  instrument state: %s ", result['result'] )
        resultDict = result['result']
        log.info("IIServiceTest test_createInstrument  INSTRUMENT INTERVAL: %s ",resultDict[(DriverChannel.INSTRUMENT,'INTERVAL')] )

        """


        """
        origparams = {}
        params[(DriverChannel.INSTRUMENT,'NAVG')] = 1
        params[(DriverChannel.INSTRUMENT,'INTERVAL')] = 4

        result = yield self.iic.setInstrumentState(instrument_id, origparams)
        log.info("IIServiceTest test_createInstrument  instrument state: %s ", result['result'] )

        # Verify the set changes were made.
        result = yield self.iic.getInstrumentState(instrument_id)
        log.info("IIServiceTest test_createInstrument  instrument state: %s ", result['result'] )
        resultDictFinal = result['result']
        log.info("IIServiceTest test_createInstrument  INSTRUMENT INTERVAL: %s ",resultDictFinal[(DriverChannel.INSTRUMENT,'INTERVAL')] )
        """


        self.sub = DataEventSubscriber(process=self.sup, origin=instrument_agent_process_id)
        log.info('IIServiceTest test_createInstrument  set handler for DataEventSubscriber')
        self.sub.ondata = self.handle_update_event    # need to do something with the data when it is received
        #yield self.sub.register()
        yield self.sub.initialize()
        yield self.sub.activate()
        log.info('IIServiceTest test_createInstrument DatasetSupplementAddedEvent activation complete')

        # Start autosampling.
        result = yield self.iic.startAutoSampling(instrument_id)
        log.info("IIServiceTest test_createInstrument  startAutoSampling: %s ", result['result'] )
        resultDict = result['result']

        # Wait for a few samples to arrive.
        yield pu.asleep(30)

        # Stop autosampling.
        result = yield self.iic.stopAutoSampling(instrument_id)
        log.info("IIServiceTest test_createInstrument  Stop autosampling: %s ", result['result'] )
        resultDict = result['result']

        log.info("IIServiceTest test_createInstrument Finished testing: Create instrument from UI")


"""

class TestInstMgmtRT(IonTestCase):

    #Testing instrument integation service in end-to-end roundtrip mode

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {
                'name':'instmgmt',
                'module':'ion.services.sa.instrument_integration_service ',
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
        self.imc = InstrumentIntegrationClient(proc=sup)



    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_getStatus(self):
        #Get status back from instrument agent associated with instrument id
        #res = yield self.imc.get_instrument_state(self.inst_id)
        #self.assertNotEqual(res, None)
        #log.info("Instrument status: " +str(res))
        log.info("IIServiceTest test_getStatus completed")

    @defer.inlineCallbacks
    def test_AutoSampling(self):
        #Execute command through instrument agent associated with instrument id

        #res = yield self.imc.execute_command(self.inst_id, 'start', [1])
        #log.info("Command result 1" +str(res))
    
        log.info("IIServiceTest test_AutoSampling completed")

    @defer.inlineCallbacks
    def test_startAgent(self):
        #Start the agent with all

        log.info("IIServiceTest test_startAgent Now testing: Create instrument from UI")
        userUpdate = {'manufacturer' : "SeaBird Electronics",
                 'model' : "SBE37",
                 'serial_num' : "1234",
                 'fw_version' : "1"}

        result = yield self.imc.createNewInstrument(userUpdate)
        log.info("IIServiceTest test_startAgent  instrument id: %s ", result['instrument_id'] )

        result = yield self.imc.startInstrumentAgent("SeaBird Electronics", result['instrument_id'], "SBE37")
        log.info("IIServiceTest test_create_instrument  instrument agent id: %s ", result['instrument_agent_id'] )

        dataDict = "conductivity:0.3444;pressure:0.3732;temperature:28.0;sound velocity:3838.3;salinity:0.993;time:(15,33,30);date:(2011,5,5)"

        pubDataEvent = InstrumentDataEventPublisher(process=self.sup, origin="magnet_topic") # all publishers/subscribers need a process associated
        yield pubDataEvent.initialize()
        yield pubDataEvent.activate()

        log.info("IIServiceTest test_startAgent  publish event")

        yield pubDataEvent.create_and_publish_event(origin=result['instrument_agent_id'],
                                           datasource_id="dataresrc123",
                                           data_block=dataDict)

        yield pubDataEvent.create_and_publish_event(origin=result['instrument_agent_id'],
                                           datasource_id="dataresrc123",
                                           data_block=dataDict)

        log.info("IIServiceTest test_startAgent  publish event completed")

        yield pu.asleep(3.0)
"""