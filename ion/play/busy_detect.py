#!/usr/bin/env python

''' Proof of concept for detecting that a deferred in stuck in a busy state. '''

from twisted.internet import defer, reactor
import signal
import time
import traceback

# Monkey-patch the reactor to count iterations, to show it's not blocked. Int overflow shouldn't break it.
reactorStepCount = 0
_doIteration = getattr(reactor, 'doIteration')
def doIterationWithCount(*args, **kwargs):
    global reactorStepCount
    reactorStepCount += 1
    _doIteration(*args, **kwargs)
setattr(reactor, 'doIteration', doIterationWithCount)

lastReactorStepCount = 0
def alarm_handler(signum, frame):
    global lastReactorStepCount
    signal.alarm(1)

    if reactorStepCount == lastReactorStepCount:
        print 'CRITICAL> Busy loop detected! The reactor has not run for >= 1 sec. ' +\
              'Currently at %s():%d of file %s.' % (
               frame.f_code.co_name, frame.f_lineno, frame.f_code.co_filename)

    lastReactorStepCount = reactorStepCount

def sleep(secs):
    def deferLaterCancel(deferred):
       delayedCall.cancel()
    d = defer.Deferred(deferLaterCancel)
    delayedCall = reactor.callLater(secs, d.callback, None)
    return d

@defer.inlineCallbacks
def do_stuff(how_many_sec, busy_loop=False):
    time_start, time_diff = time.time(), 0
    while time_diff < how_many_sec:
        if not busy_loop: yield sleep(0.05)
        time_diff = time.time() - time_start

    yield None

if __name__ == '__main__':
    signal.signal(signal.SIGALRM, alarm_handler)
    signal.alarm(1)
    
    @defer.inlineCallbacks
    def main():
        print 'Run for two seconds, but not busy loop'
        yield do_stuff(2.0, False)

        print 'Run for two seconds, and cause busy loop'
        yield do_stuff(2.0, True)

        reactor.stop()

    reactor.callWhenRunning(main)
    reactor.run()