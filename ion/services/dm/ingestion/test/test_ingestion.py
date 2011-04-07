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

from net.ooici.core.type import type_pb2
from net.ooici.play import addressbook_pb2
from ion.core.object import gpb_wrapper

from ion.services.dm.ingestion.ingestion import IngestionClient
from ion.test.iontest import IonTestCase


class IngestionTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._start_core_services()
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'ingestion1','module':'ion.services.dm.ingestion.ingestion','class':'IngestionService'}]
        sup = yield self._spawn_processes(services)

        self._ic = IngestionClient(proc=sup)
        self.sup = sup

    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_resource_reg(self):
        
        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
        
        
        #print 'Running Ingest:'
        
        dataset_id = yield self._ic.ingest()
                
        dataset = yield self._ic.retrieve(dataset_id)
        
        #print 'Got dataset'
        #print dataset
        
        
        