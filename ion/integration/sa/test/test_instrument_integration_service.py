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

import ion.util.procutils as pu


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
            log.info('IIServiceTest.handle_update_event content dict : %s', msg)

    @defer.inlineCallbacks
    def test_createInstrument(self):
        """
        Create an instrument, create associated instrument agent and get the list of instruments
        """
        #raise unittest.SkipTest('Maurice, check to see if this should be skipped in trial.')

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



    @defer.inlineCallbacks
    def test_createAndSetInstrument(self):
        """
        Create an instrument, create associated instrument agent then get and set the status
        """
        raise unittest.SkipTest('Maurice, check to see if this should be skipped in trial.')

        log.info("IIServiceTest test_createAndSetInstrument Now testing: Create instrument from UI")
        userUpdate = {'manufacturer' : "SeaBird Electronics",
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}

        result = yield self.iic.createNewInstrument(userUpdate)
        instrument_id = result['instrument_id']
        log.info("IIServiceTest test_createAndSetInstrument  instrument id: %s ", result['instrument_id'] )

        result = yield self.iic.startInstrumentAgent("SeaBird Electronics", instrument_id, "SBE37")
        instrument_agent_process_id = result['instrument_agent_process_id']
        instrument_agent_resource_id = result['instrument_agent_resource_id']
        log.info("IIServiceTest test_createAndSetInstrument  instrument agent resource id: %s   process id: %s", instrument_agent_resource_id, instrument_agent_process_id )


        result = yield self.iic.getInstrumentState(instrument_id)
        log.info("IIServiceTest test_createInstrument  instrument state: %s ", result['result'] )
        resultDict = result['result']
        log.info("IIServiceTest test_createAndSetInstrument  INSTRUMENT INTERVAL 1: %s ",resultDict[(DriverChannel.INSTRUMENT,'INTERVAL')] )


        # Set a few parameters. This will test the device set functions
        # and set up the driver for sampling commands.
        params = {}
        params[(DriverChannel.INSTRUMENT,'NAVG')] = 1
        params[(DriverChannel.INSTRUMENT,'INTERVAL')] = 4

        result = yield self.iic.setInstrumentState(instrument_id, params)
        log.info("IIServiceTest test_createAndSetInstrument  instrument state: %s ", result['result'] )


        # Verify the set changes were made.
        result = yield self.iic.getInstrumentState(instrument_id)
        #log.info("IIServiceTest test_createInstrument  instrument state: %s ", result['result'] )
        resultDict = result['result']
        intervalReturned = resultDict[(DriverChannel.INSTRUMENT,'INTERVAL')][1]
        log.info("IIServiceTest test_createAndSetInstrument  INSTRUMENT INTERVAL 2: %s ",intervalReturned)

        if intervalReturned != 4:
            self.fail('InstrumentIntegrationServiceTest: test_createAndSetInstrument returned incorrect interval after set operation.')

        origparams = {}
        origparams[(DriverChannel.INSTRUMENT,'NAVG')] = 1
        origparams[(DriverChannel.INSTRUMENT,'INTERVAL')] = 5

        result = yield self.iic.setInstrumentState(instrument_id, origparams)

        # Verify the set changes were made.
        result = yield self.iic.getInstrumentState(instrument_id)
        resultDictFinal = result['result']
        intervalReturned = resultDictFinal[(DriverChannel.INSTRUMENT,'INTERVAL')][1]
        log.info("IIServiceTest test_createAndSetInstrument  INSTRUMENT INTERVAL 3: %s ", intervalReturned)

        if intervalReturned != 5:
            self.fail('InstrumentIntegrationServiceTest: test_createAndSetInstrument returned incorrect interval after set operation.')


    @defer.inlineCallbacks
    def test_createInstrumentStartSampling(self):
        """
        Create an instrument, create associated instrument agent, start sampling and catch the events
        """
        raise unittest.SkipTest('Maurice, check to see if this should be skipped in trial.')
        
        log.info("IIServiceTest test_createInstrumentStartSampling Now testing: Create instrument from UI")
        userUpdate = {'manufacturer' : "SeaBird Electronics",
                 'model' : "unknown model",
                 'serial_num' : "1234",
                 'fw_version' : "1"}

        result = yield self.iic.createNewInstrument(userUpdate)
        instrument_id = result['instrument_id']
        log.info("IIServiceTest test_createInstrumentStartSampling  instrument id: %s ", result['instrument_id'] )

        result = yield self.iic.startInstrumentAgent("SeaBird Electronics", instrument_id, "SBE37")
        instrument_agent_process_id = result['instrument_agent_process_id']
        instrument_agent_resource_id = result['instrument_agent_resource_id']
        log.info("IIServiceTest test_createInstrumentStartSampling  instrument agent resource id: %s   process id: %s", instrument_agent_resource_id, instrument_agent_process_id )

        #self.sub = DataEventSubscriber(process=self.sup, origin=instrument_agent_process_id)  DataBlockEventSubscriber
        self.sub = DataBlockEventSubscriber(process=self.sup, origin=instrument_agent_process_id)
        log.info('IIServiceTest test_createInstrumentStartSampling  set handler for DataEventSubscriber')
        self.sub.ondata = self.handle_update_event    # need to do something with the data when it is received
        #yield self.sub.register()
        yield self.sub.initialize()
        yield self.sub.activate()
        log.info('IIServiceTest test_createInstrumentStartSampling DatasetSupplementAddedEvent activation complete')

        # Start autosampling.
        result = yield self.iic.startAutoSampling(instrument_id)
        log.info("IIServiceTest test_createInstrumentStartSampling  startAutoSampling: %s ", result['result'] )
        resultDict = result['result']

        # Wait for a few samples to arrive.
        yield pu.asleep(20)

        # Stop autosampling.
        result = yield self.iic.stopAutoSampling(instrument_id)
        log.info("IIServiceTest test_createInstrumentStartSampling  Stop autosampling: %s ", result['result'] )
        resultDict = result['result']

        log.info("IIServiceTest test_createInstrumentStartSampling Finished testing: Create instrument from UI")

