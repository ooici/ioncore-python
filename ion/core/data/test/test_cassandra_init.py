#!/usr/bin/env python

"""
@file ion/data/test/test_cassandra_init.py
@author David Stuebe

"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from uuid import uuid4

from twisted.trial import unittest
from twisted.internet import defer

from ion.core.process import process
from ion.util import procutils as pu
from ion.core.data import cassandra_bootstrap
from ion.core.data import storage_configuration_utility
from ion.test.iontest import IonTestCase

from telephus.cassandra.ttypes import InvalidRequestException, KsDef


from ion.core import ioninit
CONF = ioninit.config(__name__)


from ion.util.itv_decorator import itv



class CassandraInitTest(IonTestCase):


    keyspace = 'TESTING_KEYSPACE'

    @itv(CONF)
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        self.uname = CONF.getValue('cassandra_username', None)
        self.pword = CONF.getValue('cassandra_password', None)

        storage_conf = storage_configuration_utility.get_cassandra_configuration(self.keyspace)

        # Use a test harness cassandra client to set it up the way we want it for the test and tear it down
        test_harness = cassandra_bootstrap.CassandraSchemaProvider(self.uname, self.pword, storage_conf, error_if_existing=False)

        test_harness.connect()

        self.test_harness = test_harness
        
        try:
            yield self.test_harness.client.system_drop_keyspace(self.keyspace)
        except InvalidRequestException, ire:
            log.info(ire)



    @itv(CONF)
    @defer.inlineCallbacks
    def tearDown(self):

        try:
            yield self.test_harness.client.system_drop_keyspace(self.keyspace)
        except InvalidRequestException, ire:
            log.info(ire)

        self.test_harness.disconnect()

        yield self._shutdown_processes()
        yield self._stop_container()


    @itv(CONF)
    @defer.inlineCallbacks
    def test_run_once(self):

        spargs = {'cassandra_username':self.uname, 'cassandra_password':self.pword, 'keyspace':self.keyspace, 'error_if_existing':True}

        cip = cassandra_bootstrap.CassandraInitializationProcess(spawnargs=spargs)

        yield cip.spawn()


        ks = yield self.test_harness.client.describe_keyspace(self.keyspace)

        self.assertEqual(ks.name, self.keyspace)


    @itv(CONF)
    @defer.inlineCallbacks
    def test_fail_existing(self):

        ks_dict = storage_configuration_utility.base_ks_def.copy()
        ks_dict['name'] = self.keyspace
        ks_dict['cf_defs'] = []
        ks = KsDef(**ks_dict)

        try:
            yield self.test_harness.client.system_add_keyspace(ks)
        except InvalidRequestException, ire:
            log.info(ire)


        spargs = {'cassandra_username':self.uname, 'cassandra_password':self.pword, 'keyspace':self.keyspace, 'error_if_existing':True}

        cip = cassandra_bootstrap.CassandraInitializationProcess(spawnargs=spargs)


        self.failUnlessFailure(cip.spawn(), ion.core.data.cassandra_bootstrap.CassandraSchemaError)


    @itv(CONF)
    @defer.inlineCallbacks
    def test_pass_existing(self):

        ks_dict = storage_configuration_utility.base_ks_def.copy()
        ks_dict['name'] = self.keyspace
        ks_dict['cf_defs'] = []
        ks = KsDef(**ks_dict)

        try:
            yield self.test_harness.client.system_add_keyspace(ks)
        except InvalidRequestException, ire:
            log.info(ire)


        spargs = {'cassandra_username':self.uname, 'cassandra_password':self.pword, 'keyspace':self.keyspace, 'error_if_existing':False}

        cip = cassandra_bootstrap.CassandraInitializationProcess(spawnargs=spargs)


        yield cip.spawn()

        storage_conf = storage_configuration_utility.get_cassandra_configuration(self.keyspace)

        ks_conf = cassandra_bootstrap.build_telephus_ks(storage_conf[cassandra_bootstrap.PERSISTENT_ARCHIVE])

        ks = yield self.test_harness.client.describe_keyspace(self.keyspace)

        self.assertEqual(ks.name, ks_conf.name)
        self.assertEqual(len(ks.cf_defs), len(ks_conf.cf_defs))

        #yield cip.terminate_when_active()


