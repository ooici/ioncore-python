#!/usr/bin/env python

"""
@file ion/test/load_runner.py
@author Michael Meisinger
@brief Spawns a number of Unix processes to create some load
"""

#import ion.util.ionlog
#log = ion.util.ionlog.getLogger(__name__)

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
                    ]
    optFlags = [
                ["suite", "s", "Run test suite"],
                ["proc", "p", "Spawn sub-processes for individual tests"],
                ]

    def parseArgs(self, *args):
        self['test_args'] = args

    def opt_version(self):
        from ion.core.ionconst import VERSION
        log.info("ION  version: "+ VERSION)
        sys.exit(0)

class LoadTestRunner(object):

    @defer.inlineCallbacks
    def start_load_suite(self, suitecls, spawn_procs, options):
        print "Start load suite %s as procs:%s" % (suitecls, spawn_procs)
        if spawn_procs:
            pass
        else:
            number = int(options['count'])
            deflist = []
            for i in range(number):
                load_proc = suitecls()
                #defer.maybeDeferred(load_proc.setUp)
                d = defer.maybeDeferred(load_proc.generate_load)
                deflist.append(d)

            dl = defer.DeferredList(deflist)
            yield dl

    def start_load1(self, script, numprocs, *args):
        procs = []

        print "starting %s procs" % numprocs
        t1 = time.time()
        load_script = ['python', script]
        load_script.extend(args)

        for i in range(numprocs):

            p = subprocess.Popen(load_script)
            procs.append(p)

        print "started all procs"

        for p in procs:
            p.wait()

        t2 = time.time()
        print "all procs dead, elapsed = %s" % (t2-t1)

    @defer.inlineCallbacks
    def start_load_proc(self, script, numprocs, *args):
        pass

    def timeout(self):
        print "TIMEOUT"
        reactor.stop()

    @defer.inlineCallbacks
    def start_load_runner(self):
        options = Options()
        try:
            options.parseOptions(sys.argv[1:])
        except usage.UsageError, errortext:
            print '%s: %s' % (sys.argv[0], errortext)
            print '%s: Try --help for usage details.' % (sys.argv[0])
            sys.exit(1)

        test_class = pu.get_class(options['class'])
        print "Load test class", test_class
        assert issubclass(test_class, LoadTest), "Class must be LoadTest"

        timeout = int(options['timeout'])
        if timeout > 0:
            reactor.callLater(timeout, self.timeout)

        if options['suite']:
            yield self.start_load_suite(test_class, options['proc'], options)
        else:
            yield self.start_load_proc(test_class, options)

        reactor.stop()

if __name__ == '__main__':
    testrunner = LoadTestRunner()
    reactor.callWhenRunning(testrunner.start_load_runner)
    print "Starting reactor"
    reactor.run( )
    print "Reactor stopped"
