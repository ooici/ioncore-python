#!/usr/bin/env python

"""
@file ion/services/dm/presentation/test/test_presentation_service.py
@author David Stuebe
"""

import logging
log = logging.getLogger(__name__)

from twisted.trial import unittest

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.dm.presentation.presentation_service import PresentationClient, PresentationService

class PresentationTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 30
        services = [
            {'name': 'presentation_service',
             'module': 'ion.services.dm.presentation.presentation_service',
             'class':'PresentationService'},           
        ]
        sup = yield self._spawn_processes(services)
        self.pc = PresentationClient(proc=sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantiation_only(self):
        pass

    def test_present_catalog(self):
        """
        What?
        """
        raise unittest.SkipTest('Not implemented')

        