#!/usr/bin/env python

"""
@file ion/services/cei/test/test_provisioner.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import logging
import uuid
from twisted.internet import defer

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
    def test_provisioner(self):
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
        
        launch_id = _new_id()
        deployable_type = 'base-cluster'
        nodes = {'head-node' : FakeLaunchItem(1, 'nimbus-test', 'small', None),
                'worker-node' : FakeLaunchItem(3, 'nimbus-test', 'small', None)}
        
        client = ProvisionerClient(pid=pId)
        yield client.provision(launch_id, deployable_type, nodes)

        ok = yield notifier.wait_for_state(states.PENDING)
        self.assertTrue(ok)
        
        ok = yield notifier.wait_for_state(states.STARTED, runfirst=client.query)
        self.assertTrue(ok)

        yield client.terminate_launches(launch_id)
        
        ok = yield notifier.wait_for_state(states.TERMINATED, 
                runfirst=client.query)
        self.assertTrue(ok)

class FakeProvisionerNotifier(object):

    nodes = {}
    nodes_rec_count = {}

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
                logging.debug('Got dupe state for node %s: %s', node_id, state)
            elif old_state < state:
                self.nodes[node_id] = record
                self.nodes_rec_count[node_id] += 1
                logging.debug('Got updated state record for node %s: %s -> %s',
                        node_id, old_state, state)
            else:
                logging.debug('Got out-of-order record for node %s. %s -> %s', 
                        node_id, old_state, state)
        else:
            self.nodes[node_id] = record
            self.nodes_rec_count[node_id] = 1
            logging.debug('Recorded new state record for node %s: %s', 
                    node_id, state)
        return defer.succeed(None)

    @defer.inlineCallbacks
    def send_records(self, records, subscribers, operation='node_status'):
        for record in records:
            yield self.send_record(record, subscribers, operation)

    def assure_state(self, state):
        """Checks that all nodes have the same state.
        """
        if len(self.nodes) == 0:
            return False
        for node in self.nodes.itervalues():
            if node['state'] != state:
                return False
        return True

    @defer.inlineCallbacks
    def wait_for_state(self, state, poll=5, timeout=30, runfirst=None):
        elapsed = 0
        if runfirst:
            yield runfirst()
        while not self.assure_state(state) and elapsed < timeout:
            yield pu.asleep(poll)
            elapsed += poll
            if runfirst:
                yield runfirst()
        win = self.assure_state(state)
        if win:
            logging.debug('All nodes in %s state', state)
        else:
            logging.warn('Timeout hit before all nodes hit %s state!', state)
        defer.returnValue(win)


class FakeLaunchItem(object):
    def __init__(self, count, site, allocation_id, data):
        self.instance_ids = [str(uuid.uuid4()) for i in range(count)]
        self.site = site 
        self.allocation_id = allocation_id
        self.data = data
