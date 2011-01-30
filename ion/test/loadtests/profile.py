import cProfile
import pstats
import sys
from twisted.internet import defer, protocol, reactor
from ion.test.load_runner import LoadTestRunner

testrunner = LoadTestRunner()
def run():
    reactor.callWhenRunning(testrunner.load_runner_main)
    reactor.run()

cProfile.run('run()', 'brokerload')
p = pstats.Stats('brokerload')
p.sort_stats('cumulative').print_stats(30)
p.sort_stats('time').print_stats(30)