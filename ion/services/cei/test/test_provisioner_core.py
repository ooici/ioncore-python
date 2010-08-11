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
        self.ctx = FakeContextClient()
        self.core = ProvisionerCore(self.store, self.notifier, None, self.ctx)
    
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

    @defer.inlineCallbacks
    def test_query_ctx(self):
        launch_id = _new_id()
        launch_record = {
                'launch_id' : launch_id, 
                'state' : states.PENDING, 'subscribers' : 'fake-subscribers',
                'context' : {'uri' : 'http://fakey.com'}}
        node_records = [{'launch_id' : launch_id, 'node_id' : _new_id(),
            'state' : states.STARTED, 'public_ip' : _new_id()} for i in range(3)]

        yield self.store.put_record(launch_record)
        yield self.store.put_records(node_records)

        self.ctx.nodes = [_one_fake_ctx_node_ok(node['public_ip'], _new_id()) 
                for node in node_records]
        self.ctx.expected_count = len(node_records)
        self.complete = True
        self.error = False

        yield self.core.query_contexts()

        self.assertTrue(self.notifier.assure_state(states.RUNNING))

def _one_fake_ctx_node_ok(ip, pubkey):
    identity = Mock(ip=ip, pubkey=pubkey)
    return Mock(ok_occurred=True, error_occurred=False, identities=[identity])

class FakeContextClient(object):
    def __init__(self):
        self.nodes = []
        self.expected_count = 0
        self.complete = False
        self.error = False
    
    def query(self, uri):
        response = Mock(nodes=self.nodes, expected_count=self.expected_count,
        complete=self.complete, error=self.error)
        return defer.succeed(response)


class Mock(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeEmptyNodeQueryDriver(object):
    def list_nodes(self):
        return []

def _new_id():
    return str(uuid.uuid4())

    
