#!/usr/bin/env python

"""
@file ion/util/os_process.py
@author Dave Foster <dfoster@asascience.com>
@brief Operating System process wrapper for twisted.
"""

from twisted.internet import defer, protocol, reactor
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class OSProcessError(Exception):
    """
    An exception class thrown when OSProcess spawning fails
    """

class OSProcess(protocol.ProcessProtocol):
    """
    An operating system process wrapper for twisted.

    OSProcess is a twisted safe wrapper for a process running on the operating system
    outside of a capability container. It can be used to spawn one shot tasks or long 
    running daemons.

    It is intended that you derive from this class to customize behavior, but this base
    works well for one shot commands like cp, mv, rm etc.

    It is not safe to yield on the deferred returned by spawn unless you know the process
    will terminate. Call close in order to have a timeout protect you.

    Example:
    @verbatim
        from ion.util.os_process import OSProcess
        def cb(res):
            print "\n".join(res['stdout'])

        lsproc = OSProcess(binary="/bin/ls", spawnargs=["/var"])
        lsproc.addCallback(cb)
        lsproc.spawn()
    @endverbatim

    Tips for derived implementations:
    - The deferred returned by spawn only calls back when the process ends. For a daemon,
      you may want to return a different deferred in spawn to indicate when the daemon
      is ready, called back for example on receiving text on stdout indicating ready.
    - The _close_impl method is where you want to add your custom shutdown instructions,
      which may involve something like writing to the process' stdin or closing stdin
      for graceful shutdown.

    """
    def __init__(self, binary=None, spawnargs=None, startdir=None, env=None, **kwargs):
        """
        @param  binary      The binary to run.
        @param  spawnargs   Arguments to give the binary. Does not need to have the binary as the first argument,
                            as some other mechanisms may require.
        @param  startdir    The directory to run the binary from. If left unset, the process will run from the
                            current directory.
        """
        self.errlines           = []
        self.outlines           = []

        self.binary             = binary            # binary to run; set this in a derived class
        self.spawnargs          = spawnargs if spawnargs is not None else [] # arguments to spawn after binary
        self.deferred_exited    = defer.Deferred(self._cancel)  # is called back on process end
        self.used               = False             # do not allow anyone to use again
        self.close_timeout      = None
        self.startdir           = startdir
        self.env                = env   # Environment variables, if any

    def spawn(self, binary=None, args=None):
        """
        Spawns an OS process via twisted's reactor.

        @returns    A deferred that is called back on process ending.
                    WARNING: it is not safe to yield on this deferred as the process
                    may never terminate! Use the close method to safely close a
                    process. You may yield on the deferred returned by that.
        """
        if args is None: args = []
        
        if self.used:
            raise RuntimeError("Already used this process protocol")

        if binary == None:
            binary = self.binary

        if binary == None:
            log.error("No binary specified")
            raise RuntimeError("No binary specified")

        theargs = [binary]

        # arguments passed in here always take precedence.
        if len(args) == 0:
            theargs.extend(self.spawnargs)
        else:
            theargs.extend(args)

        log.debug("OSProcess::spawn %s %s" % (str(binary), " ".join(theargs)))
        reactor.spawnProcess(self, binary, theargs, path=self.startdir, env=self.env)
        self.used = True

        return self.deferred_exited

    def _cancel(self, deferred):
        """
        Default canceller for the process. Will call _close_impl with Force set to
        true.
        """
        self._close_impl(True)

    def addCallback(self, callback, **kwargs):
        """
        Adds a callback to be called when the process exits.
        You may call this method any time.

        @param  callback    Method to be called when the process exits. This callback should
                            take two arguments, a dict named result containing the status
                            of the process (exitcode, outlines, errlines) and then a *args param
                            for the callback args specified to this method.

                            This callback will be executed for any reason that the process
                            ends - whether it ended on its own, ended as a result of close, or
                            was killed abnormally. It is your responsibility to handle all these
                            cases.

        @param  **kwargs    Additional arguments to return to the callback.
        """
        self.deferred_exited.addCallback(callback, kwargs)

    def close(self, force=False, timeout=5):
        """
        Instructs the opened process to close down.

        @param force    If true, it severs all pipes and sends a KILL signal.
                        At this point, twisted essentially forgets about the
                        process.

        @param timeout  The amount of time allowed by the process to signal it has
                        closed. Default is 5 seconds. If the process does not shut down
                        within this time, close is called again with the force param
                        set to True.
        """

        def anon_timeout():
            self._close_impl(True)

        if not self.deferred_exited.called:
            # we have to save this so we can cancel the timeout in processEnded
            self.close_timeout = reactor.callLater(timeout, anon_timeout)
            self._close_impl(force)

        # with the timeout in place, the processEnded will always be called, so its safe
        # to yield on this deferred now
        return self.deferred_exited

    def _close_impl(self, force):
        """
        Default implementation of close. Override this in your derived class.
        """
        if force:
            self.transport.loseConnection()
            self.transport.signalProcess("KILL")
        else:
            self.transport.closeStdin()
            self.transport.signalProcess("TERM")

    def connectionMade(self):
        """
        Notice that the process has started.
        This base method provides logging notice only.
        """
        log.debug("OSProcess: process started")

    def outReceived(self, data):
        """
        Output on stdout has been received.
        Stores the output in a list.
        """
        log.debug("SO: %s" % data)
        self.outlines.append(data)

    def errReceived(self, data):
        """
        Output on stderr has been received.
        Stores the output in a list.
        """
        log.debug("SE: %s" % data)
        self.errlines.append(data)

    def processEnded(self, reason):
        """
        Notice that the process has ended.
        Will callback the deferred returned by spawn with a dict containing
        the exit code, the lines produced on stdout, and the lines on stderr.
        If the exit code is non zero, the errback is raised.
        """
        ec = 0
        if hasattr(reason, 'value') and hasattr(reason.value, 'exitCode') and reason.value.exitCode != None:
            ec = reason.value.exitCode

        log.debug("OSProcess: process ended (exitcode: %d)" % ec)

        # if this was called as a result of a close() call, we need to cancel the timeout so
        # it won't try to kill again
        if self.close_timeout != None and self.close_timeout.active():
            self.close_timeout.cancel()

        # form a dict of status to be passed as the result
        cba = { 'exitcode' : ec,
                'outlines' : self.outlines,
                'errlines' : self.errlines }

        if ec != 0:
            self.deferred_exited.errback(OSProcessError(cba))
            return

        self.deferred_exited.callback(cba)


