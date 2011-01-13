#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Paul Hubbard
@test ion.services.dm.distribution.pubsub_service Test suite for revised pubsub code
"""

import ion.util.ionlog
from twisted.internet import defer

from ion.services.dm.distribution.pubsub_service import PubSubClient
from ion.services.dm.distribution.publisher_subscriber import Publisher, Subscriber
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
        self.tt_name = 'science_data'
        self.topic_name = 'coads.nc'

    def tearDown(self):
        self._stop_container()

    def test_start_stop(self):
        pass

    @defer.inlineCallbacks
    def test_topic_tree_creation(self):
        self.tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        self.failIf(self.tt_id is None)

    @defer.inlineCallbacks
    def test_bad_topic_tree(self):
        rc = yield self.psc.declare_topic_tree(None, None)
        self.failIf(rc is not None)

    @defer.inlineCallbacks
    def test_tt_create_and_query(self):
        # create a topic tree, query to look for it
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        rc = yield self.psc.query_topic_trees(self.tt_name)
        self.failIf(rc is None)
        #self.failUnless(len(rc) > 0)

    @defer.inlineCallbacks
    def test_tt_crud(self):
        # Test create/query/rm/query on topic trees
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        tt_list = yield self.psc.query_topic_trees(self.tt_name)
        rc = yield self.psc.undeclare_topic_tree(tt_id)
        self.failIf(rc is None)
        rc = yield self.psc.query_topic_trees('.+')
        self.failIf(rc is None)
        self.failIf(len(rc) > 0)

    @defer.inlineCallbacks
    def test_topics(self):
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        topic_id = yield self.psc.define_topic(tt_id, self.topic_name)
        # Verify that it was created
        self.failIf(topic_id is None)
        rc = yield self.psc.query_topics(self.tt_name, '.+')
        self.failIf(rc is None)
        self.failIf(len(rc) < 1)

    @defer.inlineCallbacks
    def test_define_publisher(self):
        tt_id = yield self.psc.declare_topic_tree(self.xs_name, self.tt_name)
        topic_id = yield self.psc.define_topic(tt_id, self.topic_name)
        pid = yield self.psc.define_publisher(tt_id, topic_id, 'phubbard')
        self.failIf(pid is None)

    def test_subscribe(self):
        sub = Subscriber(proc=self.sup)
        pass

    
