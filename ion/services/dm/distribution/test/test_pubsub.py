#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Paul Hubbard
@test ion.services.dm.distribution.pubsub_service Test suite for revised pubsub code
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.pubsub_service import PubSubClient
from ion.test.iontest import IonTestCase

log = ion.util.ionlog.getLogger(__name__)

class PST(IonTestCase):
    """
    New tests to match the updated code for R1C2
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 5
        services = [
            {'name':'pubsub_service',
             'module':'ion.services.dm.distribution.pubsub_service',
             'class':'PubSubService'}
            ]
        yield self._start_container()
        self.sup = yield self._spawn_processes(services)
        self.psc = PubSubClient(self.sup)

        self.xs_name = 'ooici'
        self.xp_name = 'science_data'
        self.tt_name = 'pfh-tree-of-dm'
        self.topic_name = 'coads.nc'

    def tearDown(self):
        self._stop_container()

    def test_start_stop(self):
        pass

    @defer.inlineCallbacks
    def test_xp(self):
        # Creation of an exchange point
        rc = yield self.psc.declare_exchange_point(self.xp_name)
        self.failIf(rc == None)

    @defer.inlineCallbacks
    def test_topic_tree_creation(self):
        self.tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        self.failUnless(self.tt_id != None)

    @defer.inlineCallbacks
    def test_bad_topic_tree(self):
        rc = yield self.psc.declare_topic_tree(None, None)
        self.failUnless(rc == None)

    @defer.inlineCallbacks
    def test_tt_create_and_query(self):
        # create a topic tree, query to look for it
        yield self.test_topic_tree_creation()
        rc = yield self.psc.query_topic_trees(self.tt_name)
        self.failUnless(len(rc) > 0)

    @defer.inlineCallbacks
    def test_tt_crud(self):
        # Test create/query/rm/query on topic trees
        yield self.test_tt_create_query()
        rc = yield self.psc.undeclare_topic_tree(self.tt_id)
        self.failIf(rc == None)
        rc = yield self.psc.query_topic_trees('.+')
        self.failIf(len(rc) > 0)

    @defer.inlineCallbacks
    def test_topics(self):
        yield self.test_xp()
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        topic_id = yield self.psc.define_topic(tt_id, self.topic_name)
        # Verify that it was created
        self.failIf(topic_id == None)
        rc = yield self.psc.query_topics(self.xp_name, '.+')
        self.failIf(len(rc) < 1)

    @defer.inlineCallbacks
    def test_define_publisher(self):
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        topic_id = yield self.psc.define_topic(tt_id, self.topic_name)
        pid = yield self.psc.define_publisher(tt_id, topic_id, 'phubbard')
        self.failUnless(pid != None)

    def test_subscribe(self):
        # @todo Hmm, should subscribe() have more args or does it span all the namespaces?
        pass

    
