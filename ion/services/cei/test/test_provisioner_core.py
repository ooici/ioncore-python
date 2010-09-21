#!/usr/bin/env python

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import os
import uuid
import time

from twisted.internet import defer
from ion.test.iontest import IonTestCase
from twisted.trial import unittest

from ion.services.cei.provisioner_core import ProvisionerCore, update_nodes_from_context
from ion.services.cei.provisioner_store import ProvisionerStore
from ion.services.cei import states
from ion.services.cei.test.test_provisioner import FakeProvisionerNotifier

class ProvisionerCoreTests(IonTestCase):
    """Testing the provisioner core functionality
    """
    def setUp(self):
        # skip this test if IaaS credentials are unavailable
        for key in ['NIMBUS_KEY', 'NIMBUS_SECRET', 
                'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
            if not os.environ.get(key):
                raise unittest.SkipTest('Test requires IaaS credentials, skipping')
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
        node_count = 3
        launch_id = _new_id()
        launch_record = _one_fake_launch_record(launch_id, states.PENDING)
        node_records = [_one_fake_node_record(launch_id, states.STARTED) 
                for i in range(node_count)]

        yield self.store.put_record(launch_record)
        yield self.store.put_records(node_records)

        self.ctx.expected_count = len(node_records)
        self.ctx.complete = False
        self.ctx.error = False

        #first query with no ctx nodes. zero records should be updated
        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_record_count(0))
        
        # all but 1 node have reported ok
        self.ctx.nodes = [_one_fake_ctx_node_ok(node_records[i]['public_ip'], 
            _new_id(),  _new_id()) for i in range(node_count-1)]

        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING))
        self.assertEqual(len(self.notifier.nodes), node_count-1)

        #last node reports ok
        self.ctx.nodes.append(_one_fake_ctx_node_ok(node_records[-1]['public_ip'],
            _new_id(), _new_id()))

        self.ctx.complete = True
        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING))
        self.assertTrue(self.notifier.assure_record_count(1))
    
    @defer.inlineCallbacks
    def test_query_ctx_error(self):
        node_count = 3
        launch_id = _new_id()
        launch_record = _one_fake_launch_record(launch_id, states.PENDING)
        node_records = [_one_fake_node_record(launch_id, states.STARTED) 
                for i in range(node_count)]

        yield self.store.put_record(launch_record)
        yield self.store.put_records(node_records)

        self.ctx.expected_count = len(node_records)
        self.ctx.complete = False
        self.ctx.error = False

        # all but 1 node have reported ok
        self.ctx.nodes = [_one_fake_ctx_node_ok(node_records[i]['public_ip'], 
            _new_id(),  _new_id()) for i in range(node_count-1)]
        self.ctx.nodes.append(_one_fake_ctx_node_error(node_records[-1]['public_ip'],
            _new_id(), _new_id()))

        ok_ids = [node_records[i]['node_id'] for i in range(node_count-1)]
        error_ids = [node_records[-1]['node_id']]

        self.ctx.complete = True
        self.ctx.error = True

        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING, ok_ids))
        self.assertTrue(self.notifier.assure_state(states.STARTED, error_ids))

    def test_update_nodes_from_ctx(self):
        launch_id = _new_id()
        nodes = [_one_fake_node_record(launch_id, states.STARTED)
                for i in range(5)]
        ctx_nodes = [_one_fake_ctx_node_ok(node['public_ip'], _new_id(), 
            _new_id()) for node in nodes]

        self.assertEquals(len(nodes), len(update_nodes_from_context(nodes, ctx_nodes)))
        
    def test_update_nodes_from_ctx_with_hostname(self):
        launch_id = _new_id()
        nodes = [_one_fake_node_record(launch_id, states.STARTED)
                for i in range(5)]
        #libcloud puts the hostname in the public_ip field
        ctx_nodes = [_one_fake_ctx_node_ok(ip=_new_id(), hostname=node['public_ip'],
            pubkey=_new_id()) for node in nodes]

        self.assertEquals(len(nodes), len(update_nodes_from_context(nodes, ctx_nodes)))

def _one_fake_launch_record(launch_id, state):
    return {'launch_id' : launch_id, 
            'state' : state, 'subscribers' : 'fake-subscribers',
            'context' : {'uri' : 'http://fakey.com'}}

def _one_fake_node_record(launch_id, state):
    return {'launch_id' : launch_id, 'node_id' : _new_id(),
            'state' : state, 'public_ip' : _new_id()}

def _one_fake_ctx_node_ok(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=True, error_occurred=False, identities=[identity])

def _one_fake_ctx_node_error(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=False, error_occurred=True, identities=[identity],
            error_code=42, error_message="bad bad fake error")

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

    
