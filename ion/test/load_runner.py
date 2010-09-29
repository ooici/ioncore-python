#!/usr/bin/env python

"""
@file ion/test/load_runner.py
@author Michael Meisinger
@brief Spawns a number of Unix processes to create some load
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import subprocess
import sys
import time

from twisted.internet import defer, reactor
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

class LoadTestRunner(object):

    def __init__(self):
        self.exit_level = 0

    @defer.inlineCallbacks
    def start_load_suite(self, suitecls, spawn_procs, options):
        print "Start load suite %s" % (suitecls)
        numprocs = int(options['count'])

        if spawn_procs:
            # Start all as separate unix processes
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

                p = subprocess.Popen(load_proc_args)
                procs.append(p)

            #print "started all procs"

            for p in procs:
                p.wait()

        else:
            # Start all in same Twisted reactor
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

        yield defer.maybeDeferred(load_proc.setUp)
        yield defer.maybeDeferred(load_proc.generate_load)

        #d3 = defer.maybeDeferred(load_proc.tearDown)

    def timeout(self):
        #print "TIMEOUT"
        if reactor.running:
            try:
                reactor.stop()
            except Exception, ex:
                pass

    @defer.inlineCallbacks
    def load_runner_main(self):
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
                yield self.start_load_proc(test_class, options['loadid'], options)
            else:
                self.errout("Wrong arguments: Neither suite nor load process")

            try:
                self.timeout_call.cancel()
            except Exception, ex:
                pass

        except SystemExit, se:
            self.exit_level = se.code

        if reactor.running:
            reactor.stop()


    def errout(self, message):
        print '%s: %s' % (sys.argv[0], message)
        sys.exit(1)

if __name__ == '__main__':
    testrunner = LoadTestRunner()
    reactor.callWhenRunning(testrunner.load_runner_main)
    #print "Starting reactor"
    r = reactor.run()
    #print "Reactor stopped"
    if testrunner.exit_level > 0:
        sys.exit(testrunner.exit_level)
