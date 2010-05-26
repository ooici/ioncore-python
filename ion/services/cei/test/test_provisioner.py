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

from ion.services.cei.provisioner import ProvisionerService

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

        request = {'deployable_type' : 'base-cluster',
                'launch_id' : _new_id(),
                'nodes' : { 
                    'head-node' : {
                        'id' : [_new_id()],
                        'site' : 'nimbus-test',
                        'allocation' : 'small',
                    },
                    'worker-node' : {
                        'id' : [_new_id(), _new_id(), _new_id()],
                        'site' : 'nimbus-test',
                        'allocation' : 'small',
                    },
                },
                'subscribers' : [],
                }
        
        yield supervisor.send(pId, "provision", request)

        yield pu.asleep(10) #async wait

