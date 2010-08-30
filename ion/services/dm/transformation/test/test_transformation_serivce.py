#!/usr/bin/env python

"""
@file ion/services/dm/transformation/test/test_transformation_service.py
@author David Stuebe
"""

import logging
log = logging.getLogger(__name__)

from twisted.trial import unittest

from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.services.dm.transformation.transformation_service import TransformationClient, TransformationService

class TransformationTester(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.timeout = 30
        services = [
            {'name': 'transformation_service',
             'module': 'ion.services.dm.transformation.transformation_service',
             'class':'TransformationService'},           
        ]
        sup = yield self._spawn_processes(services)
        self.pc = TransformationClient(proc=sup)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    def test_instantiation_only(self):
        pass

    def test_transform(self):
        """
        What?
        """
        raise unittest.SkipTest('Not implemented')

        