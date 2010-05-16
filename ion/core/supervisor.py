#!/usr/bin/env python

"""
@file ion/core/supervisor.py
@author Michael Meisinger
@brief base class for processes that supervise other processes
"""

import logging
from twisted.internet import defer
from magnet.spawnable import spawn

from ion.core.base_process import BaseProcess, ProtocolFactory, ChildProcess
import ion.util.procutils as pu

class Supervisor(BaseProcess):
    """
    Base class for a supervisor process. A supervisor is a process with the
    purpose to spawn and monitor child processes.
    """
    def set_child_processes(self, childprocs):
        self.childProcesses = childprocs

    @defer.inlineCallbacks
    def spawn_child_processes(self):
        logging.info("Spawning child processes")
        for child in self.childProcesses:
            yield child.spawn_child(self)

    @defer.inlineCallbacks
    def init_child_processes(self):
        logging.info("Sending init message to all processes")

        for child in self.childProcesses:
            yield child.init_child()

        logging.info("All processes initialized")

    def event_failure(self):
        return


# Spawn of the process using the module name
factory = ProtocolFactory(Supervisor)

"""
from ion.core import supervisor as s
s.test_sup()
"""
