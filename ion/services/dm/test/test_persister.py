#!/usr/bin/env python

"""
@file ion/services/dm/test/test_persister.py
@test ion.services.dm.persister Persister unit tests
@author Paul Hubbard
@date 6/7/10
"""
import logging
from twisted.internet import defer

from ion.services.dm.persister import PersisterClient, PersisterService

from ion.test.iontest import IonTestCase

class PersisterTester(IonTestCase):
    """
    Exercise the persister.
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_updown(self):
        services = [
            {'name': 'persister', 'module': 'ion.services.dm.persister',
             'class': 'PersisterService'},
        ]
        sup = yield self._spawn_processes(services)

        pc = PersisterClient(proc=sup)
        rc = yield pc.persist_dap_dataset('none')
        logging.warn(rc)
