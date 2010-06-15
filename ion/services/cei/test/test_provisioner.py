#!/usr/bin/env python

"""
@file ion/services/cei/test/test_provisioner.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import logging
import uuid
from twisted.internet import defer
from magnet.container import Container
from magnet.spawnable import spawn

from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.cei.provisioner import ProvisionerService, ProvisionerClient
from ion.services.cei.provisioner_store import group_records

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
        procs = [
            {'name':'provisioner','module':'ion.services.cei.provisioner','class':'ProvisionerService'},
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

        yield pu.asleep(10) #async wait
        yield client.query()
        yield pu.asleep(5) #async wait
        yield client.query()
        yield pu.asleep(5) #async wait

class FakeLaunchItem(object):
    def __init__(self, count, site, allocation_id, data):
        self.instance_ids = [str(uuid.uuid4()) for i in range(count)]
        self.site = site 
        self.allocation_id = allocation_id
        self.data = data
