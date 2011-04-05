#!/usr/bin/env python

"""
@file ion/core/pack/app_supervisor.py
@author Michael Meisinger
@brief a supervisor process for CC applications
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.process.process import Process, ProcessFactory
from ion.core.process.supervisor import Supervisor
import ion.util.procutils as pu

class AppSupervisor(Supervisor):
    """
    A root supervisor for capability container applications. Supervises all
    processes in an application and represents the application.
    """

    @defer.inlineCallbacks
    def plc_activate(self):
        spawn_procs = self.spawn_args.get('spawn-procs', None)
        log.debug("spawn-procs: %r" % spawn_procs)
        if spawn_procs:
            yield self.container.spawn_processes(spawn_procs, sup=self)

# Spawn of the process using the module name
factory = ProcessFactory(AppSupervisor)
