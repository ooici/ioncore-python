#!/usr/bin/env python

"""
@file ion/services/dm
@author David Stuebe
@brief test for eoi ingestion demo
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.util import procutils as pu
from ion.services.coi.datastore_bootstrap.ion_preload_config import PRELOAD_CFG, ION_DATASETS_CFG, SAMPLE_PROFILE_DATASET_ID

from ion.core.process import process
from ion.services.dm.ingestion.ingestion import IngestionClient, SUPPLEMENT_MSG_TYPE, CDM_DATASET_TYPE, DAQ_COMPLETE_MSG_TYPE, PERFORM_INGEST_MSG_TYPE, CREATE_DATASET_TOPICS_MSG_TYPE
from ion.test.iontest import IonTestCase

from ion.services.coi.datastore_bootstrap.dataset_bootstrap import bootstrap_profile_dataset


class IngestionTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True}}},
            {'name':'ingestion1','module':'ion.services.dm.ingestion.ingestion','class':'IngestionService'}]
        self.sup = yield self._spawn_processes(services)

        self.proc = process.Process()
        yield self.proc.spawn()

        self._ic = IngestionClient(proc=self.proc)

        ingestion1 = yield self.sup.get_child_id('ingestion1')
        log.debug('Process ID:' + str(ingestion1))
        self.ingest= self._get_procinstance(ingestion1)



    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_recv_dataset(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        #print '\n\n\n Starting Test \n\n\n\n'
        # Reach into the ingestion service and fake the receipt of a perform ingest method - so we can test recv_dataset

        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID

        yield self.ingest._prepare_ingest(content)

        #print '\n\n\n Got Dataset in Ingest \n\n\n\n'


        cdm_dset_msg = yield self.proc.message_client.create_instance(CDM_DATASET_TYPE)

        #print '\n\n\n made Message \n\n\n\n'


        yield bootstrap_profile_dataset(cdm_dset_msg, random_initialization=True)

        #print '\n\n\n Filled out message with a dataset \n\n\n\n'
        self._ic.send_dataset(SAMPLE_PROFILE_DATASET_ID, cdm_dset_msg)

        yield pu.asleep(1)

        self.assertEqual(self.ingest.dataset.ResourceLifeCycleState, self.ingest.dataset.UPDATE)



    @defer.inlineCallbacks
    def test_recv_chunk(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        #print '\n\n\n Starting Test \n\n\n\n'
        # Reach into the ingestion service and fake the receipt of a perform ingest method - so we can test recv_dataset

        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID

        yield self.ingest._prepare_ingest(content)

        self.ingest.dataset.CreateUpdateBranch()

        #print '\n\n\n Got Dataset in Ingest \n\n\n\n'

        cdm_dset_msg = yield self.proc.message_client.create_instance(CDM_DATASET_TYPE)

        #print '\n\n\n made Message \n\n\n\n'


        yield bootstrap_profile_dataset(cdm_dset_msg, random_initialization=True)

        #print '\n\n\n Filled out message with a dataset \n\n\n\n'
        #self._ic.send_dataset(SAMPLE_PROFILE_DATASET_ID, cdm_dset_msg)




