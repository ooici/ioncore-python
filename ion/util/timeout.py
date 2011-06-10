#!/usr/bin/env python
"""
@file ion/util/timeout.py
@author David Stuebe
@brief A timeout decorator method
"""

from twisted.internet import defer, reactor

from ion.core.exception import IonError

from ion.util import procutils as pu

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from ion.util.ionlog import  logging

class TimeoutError(IonError):
    """Raised when time expires in timeout decorator"""

def timeout(secs):
    """
    Decorator to add timeout to Deferred calls
    https://gist.github.com/735556
    Credit to theduderog
    """
    def wrap(func):
        @defer.inlineCallbacks
        def _timeout(*args, **kwargs):

            if log.getEffectiveLevel() <= logging.DEBUG:
                s_args = pu.pprint_to_string(args)
                s_kwargs = pu.pprint_to_string(kwargs)
                log.debug('Setting Timeout for function "%s" to %f seconds: \nArgs: \n%s\nKWArgs: \n%s' % (func.__name__, secs, s_args, s_kwargs ))


            rawD = func(*args, **kwargs)
            if not isinstance(rawD, defer.Deferred):
                defer.returnValue(rawD)

            timeoutD = defer.Deferred()
            timesUp = reactor.callLater(secs, timeoutD.callback, None)

            try:
                rawResult, timeoutResult = yield defer.DeferredList([rawD, timeoutD], fireOnOneCallback=True, fireOnOneErrback=True, consumeErrors=True)
            except defer.FirstError, e:
                #Only rawD should raise an exception
                assert e.index == 0
                timesUp.cancel()
                e.subFailure.raiseException()
            else:
                #Timeout
                if timeoutD.called:
                    log.debug('Cancelling function callback')
                    rawD.cancel()

                    if log.getEffectiveLevel() <= logging.INFO:    # only output all this stuff when debugging

                        s_args = pu.pprint_to_string(args)
                        s_kwargs = pu.pprint_to_string(kwargs)
                        log.error('Timeout error in function "%s"\nArgs: \n%s\nKWArgs: \n%s' % (func.__name__, s_args, s_kwargs ))

                    raise TimeoutError("%s secs have expired in func name %s" % (secs, func.__name__))

            #No timeout
            log.debug('Cancelling timeout callback')

            timesUp.cancel()
            defer.returnValue(rawResult)
        return _timeout
    return wrap
