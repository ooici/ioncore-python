#!/usr/bin/env python

"""
@file ion/services/dm/preservation/test/test_cassandra_manager_service.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.core.object import workbench

from ion.services.dm.preservation.cassandra_manager_agent import CassandraManagerClient
from ion.core.object import object_utils
cassandra_keyspace_type = object_utils.create_type_identifier(object_id=2506, version=1)

class CassandraManagerTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 30
        services = [
            {'name': 'preservation_registry',
             'module': 'ion.services.dm.preservation.preservation_registry',
             'class':'PreservationRegistryService'},
            {'name': 'cassandra_manager_agent',
             'module': 'ion.services.dm.preservation.cassandra_manager_agent',
             'class':'CassandraManagerAgent'},           
        ]
        sup = yield self._spawn_processes(services)
        self.client = CassandraManagerClient(proc=sup)
        self.wb = workbench.WorkBench('No Process: Testing only')
        
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantiation_only(self):
        pass

    @defer.inlineCallbacks
    def test_create_archive(self):

        persistent_archive_repository, cassandra_keyspace  = self.wb.init_repository(cassandra_keyspace_type)
        cassandra_keyspace.name = 'ManagerServiceKeyspace'
        self.keyspace = cassandra_keyspace
        yield self.client.create_persistent_archive(self.keyspace)
        

        
