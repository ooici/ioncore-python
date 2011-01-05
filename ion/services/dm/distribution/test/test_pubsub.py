#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_pubsub.py
@author Michael Meisinger
@author David Stuebe
@author Matt Rodriguez
@brief test service for registering topics for data publication & subscription
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
#import time
from twisted.internet import defer



from ion.core import bootstrap
from ion.core.exception import ReceivedError
from ion.services.dm.distribution.pubsub_service import PubSubClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import PublisherResource,\
    PubSubTopicResource, SubscriptionResource,DataMessageObject

from ion.services.dm.distribution import base_consumer

class PST(IonTestCase):
    """
    New tests to match the updated code for R1C2
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.timeout = 5
        services = [
#            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'PubSubService'}
            ]
        yield self._start_container()
        self.sup = yield self._spawn_processes(services)
        self.psc = PubSubClient(self.sup)

    def tearDown(self):
        self._stop_container()

    def test_start_stop(self):
        pass

    @defer.inlineCallbacks
    def test_topic_tree(self):
        treename = 'pfh-tree-of-dm'
        xs_name = 'ooici'
        rc = yield self.psc.declare_topic_tree(xs_name, treename)
        self.failUnless(rc != None)

    def test_topics(self):
        # create, remove, query
        pass

    def test_publisher(self):
        # Create, check for valid return
        pass

    def test_subscribe(self):
        # Create, add listener, stream, unsub
        pass

    
