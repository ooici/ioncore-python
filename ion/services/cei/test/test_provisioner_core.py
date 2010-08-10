#!/usr/bin/env python

import logging
import uuid
import time

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.cei.provisioner_core import ProvisionerCore
from ion.services.cei.provisioner_store import ProvisionerStore
from ion.services.cei import states
from ion.services.cei.test.test_provisioner import FakeProvisionerNotifier

class ProvisionerCoreTests(IonTestCase):
    """Testing the provisioner core functionality
    """
    def setUp(self):
        self.notifier = FakeProvisionerNotifier()
        self.store = ProvisionerStore()
        self.core = ProvisionerCore(self.store, self.notifier, None)
    
    def tearDown(self):
        self.notifier = None
        self.store = None
        self.core = None

    @defer.inlineCallbacks
    def test_query_missing_node_within_window(self):
        launch_id = _new_id()
        record = {
                'launch_id' : launch_id, 'node_id' : _new_id(), 
                'state' : states.PENDING, 'subscribers' : 'fake-subscribers'}
        yield self.store.put_record(record)
        record = yield self.store.get_launch(launch_id)

        yield self.core.query_one_site('fake-site', [record], 
                driver=FakeEmptyNodeQueryDriver())
        self.assertEqual(len(self.notifier.nodes), 0)
    
    @defer.inlineCallbacks
    def test_query_missing_node_past_window(self):
        launch_id = _new_id()
        record = {
                'launch_id' : launch_id, 'node_id' : _new_id(), 
                'state' : states.PENDING, 'subscribers' : 'fake-subscribers'}
        ts = str(int(time.time() - 120 * 1e6))
        yield self.store.put_record(record, timestamp=ts)
        record = yield self.store.get_launch(launch_id)
        yield self.core.query_one_site('fake-site', [record], 
                driver=FakeEmptyNodeQueryDriver())
        self.assertEqual(len(self.notifier.nodes), 1)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

class FakeEmptyNodeQueryDriver(object):
    def list_nodes(self):
        return []

def _new_id():
    return str(uuid.uuid4())

    
