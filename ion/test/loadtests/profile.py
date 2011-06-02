import cProfile
import pstats
import re
from cStringIO import StringIO
from guppy import hpy

from twisted.internet import defer, protocol, reactor, threads
from ion.test.load_runner import LoadTestRunner

totalTimeRe = re.compile('([0-9.]+) CPU seconds')

def percentTime(name, pattern, printResult=False):
    ''' Find total CPU usage for functions/files matching the given pattern. '''

    stream = StringIO()
    p = pstats.Stats(name, stream=stream)
    p.sort_stats('time')
    p.print_stats(pattern)
    output = stream.getvalue()
    
    lines = [line for line in output.split('\n') if len(line)]
    total = float(totalTimeRe.search(lines[1]).group(1))
    lines = lines[5:] # Skip the headers
    table = [line.split() for line in lines if len(line)]
    spent = sum([float(row[1]) for row in table])
    percent = spent/total*100

    if printResult:
        print '"%s" matches %.2f%% of the CPU time' % (pattern, percent)

    return percent

testrunner = LoadTestRunner()
def run():
    reactor.callWhenRunning(testrunner.load_runner_main)
    reactor.run()


#run()
memoryOrCpu = 'cpu'

if memoryOrCpu == 'memory':
    from guppy import hpy
    
    h = hpy()
    
    def dumpHeap():
        h.dumph('out.pb')

    def showHeap():
        print h.heap()

    run()

    #showHeap()
    #h.pb('out.pb')
    hh = h.heap().get_rp(40)
    for i in range(5):
        print hh
        hh = hh.more

elif memoryOrCpu == 'cpu':
    #threads.deferToThread(dumpHeap)
    #reactor.callLater(5, showHeap)
    cProfile.run('run()', 'brokerload')

    pstats.Stats('brokerload').sort_stats('time').print_stats(100)
    #pstats.Stats('brokerload').sort_stats('cumulative').print_callers('ListFields')
    #pstats.Stats('brokerload').sort_stats('cumulative').print_stats(100)
    #pstats.Stats('brokerload').sort_stats('time').print_stats('google/protobuf')
    #pstats.Stats('brokerload').sort_stats('cumulative').print_stats('cache.py')
    #pstats.Stats('brokerload').sort_stats('cumulative').print_callers('isinstance')
    #pstats.Stats('brokerload').sort_stats('cumulative').print_stats('(gpb_wrapper|object_utils)')
    #pstats.Stats('brokerload').sort_stats('cumulative').print_stats('cache.py')


    #percentTime('brokerload', 'gpb_wrapper', printResult=True)
    percentTime('brokerload', 'protobuf', printResult=True)
    #percentTime('brokerload', 'twisted', printResult=True)
    #percentTime('brokerload', '{isinstance}', printResult=True)
    #percentTime('brokerload', '{select.select}', printResult=True)
else:
    run()
