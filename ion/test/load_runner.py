#!/usr/bin/env python

"""
@file ion/test/load_runner.py
@author Michael Meisinger
@brief Spawns a number of Unix processes to create some load
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import subprocess
import sys, os
import time

from twisted.internet import defer, protocol, reactor
from twisted.python import usage

import ion.util.procutils as pu
from ion.test.loadtest import LoadTest

class Options(usage.Options):
    """
    Extra arg for file of "program"/"module" to run
    """
    synopsis = "ION load test runner options"

    longdesc = """Runs a load test or test suite.
            Start with python -m ion.test.test_runner"""

    optParameters = [
                ["class", "c", None, "Qualified load test class"],
                ["count", "n", 1, "Number of load processes"],
                ["timeout", "t", 0, "Seconds after which to kill the load processes"],
                ["loadid", "l", 0, "Identity (0-based number) of load process"],
                    ]
    optFlags = [
                ["suite", "s", "Run load test suite"],
                ["proc", "p", "Spawn individual load processes"],
                ]

    def parseArgs(self, *args):
        self['test_args'] = args
        #print "Extra args", args

    def opt_version(self):
        from ion.core.ionconst import VERSION
        print "ION version: ", VERSION
        sys.exit(0)

class TestRunnerProcessProtocol(protocol.ProcessProtocol):

    def connectionMade(self):
        print "connectionMade"
    def outReceived(self, data):
        print "outReceived! with %d bytes!" % len(data)
        print data
    def errReceived(self, data):
        print "errReceived! with %d bytes!" % len(data)
        print data
    def inConnectionLost(self):
        print "inConnectionLost! stdin is closed! (we probably did it)"
    def outConnectionLost(self):
        print "outConnectionLost! The child closed their stdout!"
        # now is the time to examine what they wrote
        #print "I saw them write:", self.data
    def errConnectionLost(self):
        print "errConnectionLost! The child closed their stderr."
    def processExited(self, reason):
        print "processExited, status %d" % (reason.value.exitCode,)
    def processEnded(self, reason):
        print "processEnded, status %d" % (reason.value.exitCode,)
        print "quitting"
        reactor.stop()

class LoadTestRunner(object):

    def __init__(self):
        self.exit_level = 0
        self.mode = None
        self.load_procs = {}
        self._shutdown_deferred = None
        self._is_shutdown = False
        self._is_kill = False

    @defer.inlineCallbacks
    def start_load_suite(self, suitecls, spawn_procs, options):
        print "Start load suite %s" % (suitecls)
        numprocs = int(options['count'])

        if spawn_procs:
            # Start all as separate unix processes
            self.mode = "suite-spawn"
            load_script = ['python', '-m', 'ion.test.load_runner', '-c', options['class']]
            timeout = int(options['timeout'])
            if timeout > 0:
                load_script.extend(['-t', str(timeout)])

            print "Spawning %s load processes: %s" % (numprocs, options['class'])

            procs = []
            for i in range(numprocs):
                load_proc_args = list(load_script)
                load_proc_args.extend(["-l", str(i)])
                load_proc_args.extend(options['test_args'])
                #print "Starting load process %s: %s" % (i, load_proc_args)

                #procProt = TestRunnerProcessProtocol()
                #p = reactor.spawnProcess(procProt,
                #            load_proc_args[0],
                #            args=load_proc_args,
                #            env=os.environ)

                p = subprocess.Popen(load_proc_args)
                procs.append(p)
                self.load_procs[str(i)] = p

            #yield pu.asleep(20)
            #print "started all procs"

            for p in procs:
                try:
                    p.wait()
                except Exception, ex:
                    print ex
                    self._is_kill = True

        else:
            # Start all in same Twisted reactor
            self.mode = "suite-internal"
            deflist = []
            for i in range(numprocs):
                d = self.start_load_proc(suitecls, str(i), options)
                deflist.append(d)

            dl = defer.DeferredList(deflist)
            yield dl

    @defer.inlineCallbacks
    def start_load_proc(self, suitecls, loadid, options):
        """
        Starts a single load in the current processes
        @retval Deferred
        """
        load_proc = suitecls()
        load_proc.load_id = str(loadid)
        load_proc.options = options

        self.load_procs[load_proc.load_id] = load_proc

        yield defer.maybeDeferred(load_proc.setUp)
        yield defer.maybeDeferred(load_proc.generate_load)

        yield defer.maybeDeferred(load_proc.tearDown)

    def timeout(self):
        #print "TIMEOUT"
        if reactor.running:
            try:
                reactor.stop()
            except Exception, ex:
                pass

    def pre_shutdown(self):
        #print "pre_shutdown"
        if self._is_shutdown:
            return
        self._shutdown_deferred = defer.Deferred()
        self._shutdown_to = reactor.callLater(2, self.shutdown_timeout)

        prockeys = sorted(self.load_procs.keys())
        for key in prockeys:
            proc = self.load_procs[key]
            if isinstance(proc, LoadTest):
                proc._shutdown = True
            else:
                pass

        return self._shutdown_deferred

    def shutdown_timeout(self):
        print "SHUTDOWN timeout"
        self._shutdown_deferred.callback(None)

    @defer.inlineCallbacks
    def load_runner_main(self):
        reactor.addSystemEventTrigger("before", "shutdown", self.pre_shutdown)

        try:
            # Parse the options
            options = Options()
            try:
                options.parseOptions(sys.argv[1:])
            except usage.UsageError, errortext:
                print '%s: %s' % (sys.argv[0], errortext)
                print '%s: Try --help for usage details.' % (sys.argv[0])
                sys.exit(1)

            if not options['class']:
                sys.exit(0)

            try:
                test_class = pu.get_class(options['class'])
            except Exception, ex:
                self.errout(str(ex))

            if not issubclass(test_class, LoadTest):
                self.errout("Class must be LoadTest")

            timeout = int(options['timeout'])
            if timeout > 0:
                self.timeout_call = reactor.callLater(timeout, self.timeout)

            if options['suite']:
                yield self.start_load_suite(test_class, options['proc'], options)
                print "Load test suite stopped."
            elif options['loadid']:
                self.mode = "load-process"
                yield self.start_load_proc(test_class, options['loadid'], options)
            else:
                self.errout("Wrong arguments: Neither suite nor load process")

            try:
                self.timeout_call.cancel()
            except Exception, ex:
                pass

        except SystemExit, se:
            self.exit_level = se.code
            print "EXIT"

        if self._shutdown_deferred:
            #print "cancel shotdown to"
            self._shutdown_to.cancel()
            self._shutdown_deferred.callback(None)

        # Need to check for kill condition. Somehow this interferes with the reactor
        if not self._is_kill and reactor.running:
            try:
                reactor.stop()
            except Exception, ex:
                pass

        self._is_shutdown = True

    def errout(self, message):
        print '%s: %s' % (sys.argv[0], message)
        sys.exit(1)

if __name__ == '__main__':
    testrunner = LoadTestRunner()
    reactor.callWhenRunning(testrunner.load_runner_main)
    r = reactor.run()

    if testrunner.exit_level > 0:
        sys.exit(testrunner.exit_level)
