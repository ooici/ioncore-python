#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Paul Hubbard
@test ion.services.dm.distribution.pubsub_service Test suite for revised pubsub code
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.pubsub_service import PubSubClient
#from ion.services.dm.distribution.publisher_subscriber import Subscriber
from ion.test.iontest import IonTestCase
from twisted.trial import unittest
from ion.util.procutils import asleep

from ion.util.itv_decorator import itv

log = ion.util.ionlog.getLogger(__name__)

class PST(IonTestCase):
    """
    New tests to match the updated code for R1C2
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 5
        services = [
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },
            {
                'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                    'spawnargs':{'servicename':'datastore'}
            },
            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry_beta.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}},
            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },

            ]
        yield self._start_container()
        self.sup = yield self._spawn_processes(services)
        self.psc = PubSubClient(self.sup)

        self.xs_name = 'swapmeet'
        self.tt_name = 'science_data'
        self.topic_name = 'http://ooici.net:8001/coads.nc'

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    def test_start_stop(self):
        pass

    @itv
    @defer.inlineCallbacks
    def test_topic_tree_creation(self):
        self.tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        self.failIf(self.tt_id is None)

    @itv
    @defer.inlineCallbacks
    def test_bad_topic_tree_delete(self):
        rc = yield self.psc.undeclare_topic_tree('fubar')
        self.failIf(rc is None)

    @itv
    @defer.inlineCallbacks
    def test_topic_tree_write_delete(self):
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, 'fubar')
        self.failIf(tt_id is None)
        yield self.psc.undeclare_topic_tree(tt_id)

    @itv
    @defer.inlineCallbacks
    def test_bad_topic_tree(self):
        raise unittest.SkipTest('Waiting for code')
        rc = yield self.psc.declare_topic_tree(None, None)
        self.failIf(rc is not None)

    @itv
    @defer.inlineCallbacks
    def test_tt_create_and_query(self):
        raise unittest.SkipTest('Waiting for code')
        # create a topic tree, query to look for it
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        self.failIf(tt_id is None)
        rc = yield self.psc.query_topic_trees(self.tt_name)
        self.failIf(rc is None)

    @itv
    @defer.inlineCallbacks
    def test_tt_crud(self):
        raise unittest.SkipTest('Waiting for code')
        # Test create/query/rm/query on topic trees
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        tt_list = yield self.psc.query_topic_trees(self.tt_name)
        self.failIf(tt_list is None)
        rc = yield self.psc.undeclare_topic_tree(tt_id)
        self.failIf(rc is None)
        rc = yield self.psc.query_topic_trees('.+')
        self.failIf(rc is None)
        self.failIf(len(rc) > 0)

    @itv
    @defer.inlineCallbacks
    def test_define_topic(self):
        tt_id = 'fake_topic_id'
        topic_id = yield self.psc.define_topic(tt_id, self.topic_name)
        # Verify that it was created
        self.failIf(topic_id is None)

    @itv
    @defer.inlineCallbacks
    def test_topics(self):
        raise unittest.SkipTest('Waiting for code')
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        topic_id = yield self.psc.define_topic(tt_id, self.topic_name)
        # Verify that it was created
        self.failIf(topic_id is None)
        rc = yield self.psc.query_topics(self.tt_name, '.+')
        self.failIf(rc is None)
        self.failIf(len(rc) < 1)

    @itv
    @defer.inlineCallbacks
    def test_define_publisher(self):
        raise unittest.SkipTest('Waiting for code')
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        topic_id = yield self.psc.define_topic(tt_id, self.topic_name)
        pid = yield self.psc.define_publisher(tt_id, topic_id, 'phubbard')
        self.failIf(pid is None)

    @itv
    def test_subscribe(self):
        raise unittest.SkipTest('Waiting for code')
        # @todo Create publisher, send data, verify receipt a la scheduler test code
        #sub = Subscriber('fake', process=self.sup)
        pass

