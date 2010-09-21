#!/usr/bin/env python

"""
@file ion/services/cei/test/test_provisioner.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import uuid
import os

from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.cei.provisioner import ProvisionerClient
import ion.services.cei.states as states

def _new_id():
    return str(uuid.uuid4())

class ProvisionerServiceTest(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def _set_it_up(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        notifier = FakeProvisionerNotifier()
        procs = [
                {'name':'provisioner','module':'ion.services.cei.provisioner',
                    'class':'ProvisionerService', 'spawnargs' : {'notifier' : notifier}},
            {'name':'dtrs','module':'ion.services.cei.dtrs','class':'DeployableTypeRegistryService'}
        ]
        yield self._declare_messaging(messaging)
        supervisor = yield self._spawn_processes(procs)

        pId = yield self.procRegistry.get("provisioner")
        
        client = ProvisionerClient(pid=pId)
        defer.returnValue((client, notifier))

    @defer.inlineCallbacks
    def test_provisioner(self):

        # skip this test if IaaS credentials are unavailable
        maybe_skip_test()

        client, notifier = yield self._set_it_up()
        
        worker_node_count = 3
        deployable_type = 'base-cluster'
        nodes = {'head-node' : FakeLaunchItem(1, 'nimbus-test', 'small', None),
                'worker-node' : FakeLaunchItem(worker_node_count, 
                    'nimbus-test', 'small', None)}
        
        launch_id = _new_id()

        node_ids = [node_id for node in nodes.itervalues() 
                for node_id in node.instance_ids]
        self.assertEqual(len(node_ids), worker_node_count + 1)

        yield client.provision(launch_id, deployable_type, nodes)

        ok = yield notifier.wait_for_state(states.PENDING, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(2))
        
        ok = yield notifier.wait_for_state(states.STARTED, node_ids, runfirst=client.query)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(3))

        yield client.terminate_launches(launch_id)
        
        ok = yield notifier.wait_for_state(states.TERMINATED, node_ids,
                runfirst=client.query)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(5))

        self.assertEqual(len(notifier.nodes), len(node_ids))

class FakeProvisionerNotifier(object):

    def __init__(self):
        self.nodes = {}
        self.nodes_rec_count = {}

    def send_record(self, record, subscribers, operation='node_status'):
        """Send a single node record to all subscribers.
        """
        record = record.copy()
        node_id = record['node_id']
        state = record['state']
        if node_id in self.nodes:
            old_record = self.nodes[node_id]
            old_state = old_record['state']
            if old_state == state:
                log.debug('Got dupe state for node %s: %s', node_id, state)
            elif old_state < state:
                self.nodes[node_id] = record
                self.nodes_rec_count[node_id] += 1
                log.debug('Got updated state record for node %s: %s -> %s',
                        node_id, old_state, state)
            else:
                log.debug('Got out-of-order record for node %s. %s -> %s', 
                        node_id, old_state, state)
        else:
            self.nodes[node_id] = record
            self.nodes_rec_count[node_id] = 1
            log.debug('Recorded new state record for node %s: %s', 
                    node_id, state)
        return defer.succeed(None)

    @defer.inlineCallbacks
    def send_records(self, records, subscribers, operation='node_status'):
        for record in records:
            yield self.send_record(record, subscribers, operation)

    def assure_state(self, state, nodes=None):
        """Checks that all nodes have the same state.
        """
        if len(self.nodes) == 0:
            return False

        if nodes:
            for node in nodes:
                if not (node in self.nodes and 
                        self.nodes[node]['state'] == state):
                    return False
            return True

        for node in self.nodes.itervalues():
            if node['state'] != state:
                return False
        return True

    def assure_record_count(self, count, nodes=None):
        if len(self.nodes) == 0:
            return count == 0

        if nodes:
            for node in nodes:
                if self.nodes_rec_count.get(node, 0) != count:
                    return False
            return True

        for node_rec_count in self.nodes_rec_count.itervalues():
            if node_rec_count != count:
                return False
        return True


    @defer.inlineCallbacks
    def wait_for_state(self, state, nodes=None, poll=5, timeout=30, runfirst=None):
        elapsed = 0
        if runfirst:
            yield runfirst()
        while not self.assure_state(state, nodes) and elapsed < timeout:
            yield pu.asleep(poll)
            elapsed += poll
            if runfirst:
                yield runfirst()
        win = self.assure_state(state, nodes)
        if win:
            log.debug('All nodes in %s state', state)
        else:
            log.warn('Timeout hit before all nodes hit %s state!', state)
        defer.returnValue(win)


class FakeLaunchItem(object):
    def __init__(self, count, site, allocation_id, data):
        self.instance_ids = [str(uuid.uuid4()) for i in range(count)]
        self.site = site 
        self.allocation_id = allocation_id
        self.data = data

def maybe_skip_test():
    """Some tests require IaaS credentials. Skip if they are not available
    """
    for key in ['NIMBUS_KEY', 'NIMBUS_SECRET', 
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
        if not os.environ.get(key):
            raise unittest.SkipTest('Test requires IaaS credentials, skipping')

