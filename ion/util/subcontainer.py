#!/usr/bin/env python

"""
@file ion/util/subcontainer.py
@author Dave Foster <dfoster@asascience.com>
@brief ion.util.subcontainer   Container running inside of a container (I N C E P T I O N)
"""

import ion.util.ionlog
from ion.util.os_process import OSProcess
from ion.core import ioninit
from twisted.internet import defer, reactor
from ion.core.process.process import Process
log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)
import os, tempfile, sys
from uuid import uuid4

class Subcontainer(Process):

    """
    Manages a capability container process, spawned as a child of the container this process is in.

    Containers are spawned with the same sysname as the owning container, so services can communicate.
    Use this class to bring up a number of containers with services in them to have a cloud-like
    simulation environment.

    Usage:
        subc = Subcontainer(appfile="res/apps/attributestore.app")      # relative to ioncore-python directory
        subc.spawn()   # when this deferred calls back, the container should be operational and your services started

        # interact
        asc = AttributeStoreClient()
        asc.put("hi", "test")
        asc.get("hi")       # after deferred calls back, will get "test" from AttributeStoreService running in our 
                            # subcontainer

        # close it down
        subc.terminate()
    """

    class CCProcess(OSProcess):

        """
        Internal OSProcess-derived class that handles the actual spawning of the container.
        This is all wrapped and used by the Subcontainer process, you shouldn't need to interact with
        it directly.
        """

        def __init__(self, binroot=None, appfile="res/apps/attributestore.app", **kwargs):
            sysname = ioninit.sys_name
            tf = os.path.join(tempfile.gettempdir(), "cc-" + str(uuid4()))

            # get path to binary, this is lame
            bintorun="bin/twistd" ##ioninit.adjust_dir("bin/twistd")
            pathtorun=ioninit.adjust_dir(".")

            spawnargs = ["-n", "--pidfile", tf + ".pid", "--logfile", tf + ".log", "cc", "-n", "-a", "sysname=%s" % sysname, appfile]

            self._defer_ready = defer.Deferred()

            OSProcess.__init__(self, binary=bintorun, spawnargs=spawnargs, startdir=pathtorun)

        def spawn(self, binary=None, args=None):
            if args is None: args = []
            self._defer_spawn = OSProcess.spawn(self, binary, args)

            # TODO: cannot figure out how to wait on stdout, it only goes to the log file and not stdout, can't figure
            # out how to do so.  We'll just timeout this deferred for now.
            reactor.callLater(3, self._defer_ready.callback, True)
            return self._defer_ready

    def __init__(self, appfile="res/apps/attributestore.app", receiver=None, spawnargs=None, **kwargs):
        """
        Initializer override. 
        @param appfile  The app file the subcontainer should run on startup. Relative to the ioncore-python directory, will likely
                        be "res/apps/<something>.app".
        """
        self._appfile = appfile
        Process.__init__(self, receiver, spawnargs, **kwargs)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        On activation override. Spawns the subcontainer.
        """
        Process.on_activate(self, *args, **kwargs)

        self._container = self.CCProcess()
        yield self._container.spawn()

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        On termination override. Closes the subcontainer.
        """
        yield self._container.close()
        yield Process.on_terminate(self, *args, **kwargs)
