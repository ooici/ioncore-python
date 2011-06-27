#!/usr/bin/env python

"""
@file ion/core/exception.py
@author Michael Meisinger
@brief tests for applications
"""

import os, sys

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.exception import ConfigurationError
from ion.core.pack.application import AppLoader, AppDefinition
from ion.core.pack.app_manager import AppManager
from ion.core.pack.release import ReleaseLoader, ReleaseDefinition
from ion.test.iontest import IonTestCase
from ion.core import ioninit

class AppLoaderTest(IonTestCase):
    """
    Tests the app loader.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        
    def test_load_appfile(self):
        # Tests run in <main>/_trial_temp
        filename = 'res/apps/example.app'
        app = AppLoader.load_app_definition(filename)
        log.debug(app)
        self.assertTrue(app)
        self.assertIsInstance(app, AppDefinition)

    def test_load_resfile(self):
        # Tests run in <main>/_trial_temp
        filename = 'res/deploy/example.rel'
        rel = ReleaseLoader.load_rel_definition(filename)
        log.debug(rel)
        self.assertTrue(rel)
        self.assertIsInstance(rel, ReleaseDefinition)

    @defer.inlineCallbacks
    def test_app_manager(self):
        filename = 'res/apps/example.app'
        container = ioninit.container_instance

        app_manager = AppManager(container)
        yield app_manager.initialize(None)
        yield app_manager.start_app(filename)
        self.assertTrue(app_manager.is_app_started('example'))

        yield app_manager.start_app(filename, start_mult=False)
        self.assertTrue(app_manager.is_app_started('example'))

        yield app_manager.stop_app('example')
        self.assertFalse(app_manager.is_app_started('example'))

        yield app_manager.start_app(None, app_name='example')
        self.assertTrue(app_manager.is_app_started('example'))
        yield app_manager.stop_app('example')

        yield app_manager.start_rel('res/deploy/example.rel')
        self.assertTrue(app_manager.is_app_started('example'))

        process_app = ('ccagent', 'ion.core.cc.cc_agent', 'CCAgent')
        yield app_manager.start_app(None, app_name='ccagent', process_app=process_app, app_config={})
        self.assertTrue(app_manager.is_app_started('ccagent'))
        yield app_manager.stop_app('ccagent')

        
        