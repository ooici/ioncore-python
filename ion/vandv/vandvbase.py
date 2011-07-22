#!/usr/bin/env python

"""
@file ion/vandv/vandvbase.py
@author Dave Foster <dfoster@asascience.com>
@brief Base class for vandv tests.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor

from ion.util.os_process import OSProcess

#from ion.core.ioninit import ion_config
from ion.core import ioninit
import os

class ITVOSProcess(OSProcess):
    """
    OSProcess for running the itv tool.

    The spawn() implementation will call back when the debug container of the itv tool has
    started, or if the whole things ends.
    """

    def __init__(self, files=None, hostname=None, sysname=None):

        self._files = files
        assert self._files and len(self._files) > 0

        self._hostname = hostname or ioninit.container_instance.exchange_manager.exchange_space.message_space.hostname   # haha
        self._sysname = sysname or ioninit.sys_name

        binary = os.path.join(os.getcwd(), "bin", "itv")

        # build arguments
        args=["--hostname=%s" % self._hostname,
              "--sysname=%s" % self._sysname]#,
        #"--lockfile=%s" % lockfile]
        args.extend(self._files)

        OSProcess.__init__(self, binary=binary, spawnargs=args)

    def spawn(self, **kwargs):
        OSProcess.spawn(self, **kwargs)
        self._ready_deferred = defer.Deferred()

        # install timeout
        self._timeout_calllater = reactor.callLater(120, self._timeout)

        def cancel_timeout(res):
            if self._timeout_calllater.active():
                self._timeout_calllater.cancel()

        self._ready_deferred.addCallback(cancel_timeout)

        return self._ready_deferred

    @defer.inlineCallbacks
    def _timeout(self):
        yield self.close()  # should call processEnded, triggering _ready_deferred's callback

    def errReceived(self, data):
        OSProcess.errReceived(self, data)

        if not self._ready_deferred.called and ("LAUNCHER" in data):
            cba = { 'exitcode': 0,
                   'outlines' : self.outlines,
                   'errlines' : self.errlines }

            self._ready_deferred.callback(cba)

    def processEnded(self, reason):
        if hasattr(self, '_ready_deferred') and not self._ready_deferred.called:
            cba = { 'exitcode': 0,
                   'outlines' : self.outlines,
                   'errlines' : self.errlines }

            self._ready_deferred.callback(cba)

        OSProcess.processEnded(self, reason)

class VVBase(object):

    def __init__(self):

        self._spawned_itvs = []

    def setup(self):
        """
        Basic setup for the entire test. Override this method in your test class.
        """
        pass

    @defer.inlineCallbacks
    def teardown(self):
        """
        Basic teardown for the entire test. Override this method in your test class.
        """
        for itv in self._spawned_itvs:
            if not itv.deferred_exited.called:
                yield itv.close()

    @defer.inlineCallbacks
    def _start_itv(self, files=None, hostname=None, sysname=None):
        """
        Helper method to start an itv container.

        Returns when the itv tool indicates it has started everything including the debug shell (which is not seen).
        """
        itv_proc = ITVOSProcess(files=files, hostname=hostname, sysname=sysname)
        yield itv_proc.spawn()

        self._spawned_itvs.append(itv_proc)

        defer.returnValue(itv_proc)


