#!/usr/bin/env python

"""
@file ion/test/loadtest.py
@author Michael Meisinger
@brief Base class for a load test
"""

import time

from twisted.internet import defer, reactor
from twisted.python import usage

#import ion.util.ionlog
#log = ion.util.ionlog.getLogger(__name__)

class LoadTestSuite(object):
    pass

class LoadTestOptions(usage.Options):
    """
    Options for test case arguments
    """
    optParameters = []
    optFlags = []

    def parseArgs(self, *args):
        self['extra_args'] = args

class LoadTest(object):
    """
    @note careful. If a unittest, will be automatically be picked up by trial
    """
    def __init__(self, *args):
        self.load_options = LoadTestOptions()
        self._shutdown = False
        self.start_state = {}
        self.base_state = {}
        self.cur_state = {}
        self.base_state['_time'] = 0.0
        self.cur_state['_time'] = time.time()
        self.start_state['_time'] = time.time()

    def setUp(self):
        """
        Hook method for setting up the test fixture before exercising it.
        """
        return defer.succeed(None)

    def tearDown(self):
        """
        Hook method for deconstructing the test fixture after testing it.
        """
        return defer.succeed(None)

    def generate_load(self):
        """
        Hook method for generating the load.
        """
        return defer.succeed(None)

    def is_shutdown(self):
        return self._shutdown

    def _set_state(self, key, value):
        self.cur_state[key] = value

    def _update_time(self):
        self.cur_state['_time'] = time.time()

    def _copy_state(self):
        oldstate = dict(self.cur_state)
        self.base_state = oldstate

    def _get_interval(self):
        return self.cur_state['_time'] - self.base_state['_time']

    def _get_rate(self, key, interval=None):
        cur_value = self.cur_state[key]
        base_value = self.base_state[key]
        interval = interval or self._get_interval()
        rate = 0
        if interval != 0:
            rate = (cur_value - base_value) / interval
        return rate

    def _get_state_rate(self):
        interval = self._get_interval()
        rate = dict(_time=interval)
        for key in self.cur_state.keys():
            if not key == "_time":
                rate[key] = self._get_rate(key, interval)
        return rate

    def _enable_monitor(self, delay):
        """
        Enables a monitor: an asynchronous call to a function that provides
        current stats.
        """
        self.monitor_delay = delay
        self.monitor_call = reactor.callLater(self.monitor_delay, self._call_monitor)
        self._update_time()
        self._copy_state()

    def _call_monitor(self, output=True):
        self.monitor_call = reactor.callLater(self.monitor_delay, self._call_monitor)
        self._update_time()
        try:
            self.monitor(output)
        except Exception, ex:
            print "Exception in load process monitor", ex

        # Set base time and state vector for next interval
        self._copy_state()

    def _disable_monitor(self):
        if self.monitor_call:
            try:
                self.monitor_call.cancel()
            except Exception, ex:
                pass

    def monitor(self):
        pass
