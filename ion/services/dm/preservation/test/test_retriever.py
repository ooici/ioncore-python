#!/usr/bin/env python

"""
@file ion/services/dm/preservation/test/test_retriever.py
@test ion.services.dm.retriever.test.test_retriever.py
@author Paul Hubbard
@date 6/11/10
"""

from ion.services.dm.preservation.retriever import RetrieverClient

import logging
log = logging.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase

class RetrieverTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 60
        services = [
            {'name' : 'retriever',
             'module':'ion.services.dm.preservation.retriever',
            'class' : 'RetrieverService'},
        ]
        sup = yield self._spawn_processes(services)
        self.rc = RetrieverClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantition_only(self):
        pass

    @defer.inlineCallbacks
    def test_cache_hit(self):
        raise unittest.SkipTest('code not completed')

        das = yield self.rc.get_url('http://amoeba.ucsd.edu:8001/coads.nc.das')
        self.failUnlessSubstring('NC_GLOBAL', das)
