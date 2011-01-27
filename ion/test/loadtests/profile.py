import cProfile
import pstats
import sys
from twisted.internet import defer, protocol, reactor
from ion.test.load_runner import LoadTestRunner

testrunner = LoadTestRunner()
reactor.callWhenRunning(testrunner.load_runner_main)
#reactor.run()
cProfile.run('r = reactor.run()', 'brokerload')
p = pstats.Stats('brokerload')
p.sort_stats('cumulative').print_stats(30)
p.sort_stats('time').print_stats(30)

if testrunner.exit_level > 0:
    sys.exit(testrunner.exit_level)