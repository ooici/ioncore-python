#!/usr/bin/env python

"""
@file ion/services/cei/test/test_dtrs.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import logging
from twisted.internet import defer
from magnet.container import Container
from magnet.spawnable import spawn

from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.cei.dtrs import DeployableTypeRegistryClient

class TestDeployableTypeRegistryService(IonTestCase):
    """Testing deployable type lookups
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_dtrs_lookup(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        procs = [
            {'name':'dtrs','module':'ion.services.cei.dtrs','class':'DeployableTypeRegistryService'},
        ]
        yield self._declare_messaging(messaging)
        supervisor = yield self._spawn_processes(procs)

        dtrsId = yield self.procRegistry.get("dtrs")
        logging.info("dtrsId: "+repr(dtrsId))

        client = DeployableTypeRegistryClient(pid=dtrsId)
        nodes = {
            'head-node' : {'site' : 'ec2-west'},
            'worker-node' : {'site' : 'nimbus-test'}}

        result = yield client.lookup('base-cluster', nodes=nodes)
        doc = result['document']
        nodes = result['nodes']
        for node in nodes.itervalues():
            self.assertTrue('iaas_image' in node)

        got_error = False
        try:
            yield client.lookup('this-image-doesnt-exist', nodes)
        except KeyError:
            got_error = True
        
        self.assertTrue(got_error)

