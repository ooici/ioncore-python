#!/usr/bin/env python

"""
@file ion/core/supervisor.py
@author Michael Meisinger
@brief base class for processes that supervise other processes
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.base_process import BaseProcess, ProcessFactory
import ion.util.procutils as pu

class Supervisor(BaseProcess):
    """
    Base class for a supervisor process. A supervisor is a process with the
    purpose to monitor child processes and to restart them in case
    of failure. Spawing child processes is a function of the BaseProcess itself.
    """

    def event_failure(self):
        return

# Spawn of the process using the module name
factory = ProcessFactory(Supervisor)

"""
from ion.core import supervisor as s
spawn(s)
"""
