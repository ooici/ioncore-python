#!/usr/bin/env python

"""
@file ion/vandv/vandvapp.py
@author Dave Foster <dfoster@asascience.com>
@brief Helper starter for v+v tests.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor

#from ion.core.ioninit import ion_config
from ion.core import ioninit
from ion.core.cc.shell import control
import inspect, re

#-- CC Application interface

# global fun!
curtest = None
curstep = 0
_teststeps = []

def _print(out):
    control.serverProtocol.write(out + "\n")

def test_info():
    global curstep, _teststeps, curtest

    _print("\n==============================================================================")
    _print(str(curtest.__class__))
    _print(str(curtest.__doc__.strip()))

    _print("\nSteps:\n")
    for idx, x in enumerate(_teststeps):
        if idx == curstep:
            prefix = "===>"
            postfix = "<==="
        else:
            prefix=""
            postfix=""

        _print("%s\t%s\t%s" % (prefix, x[1].__doc__.strip(), postfix))

    _print("\n==============================================================================\n")

@defer.inlineCallbacks
def next():
    global curstep, curtest, _teststeps

    # execute step
    yield defer.maybeDeferred(_teststeps[curstep][1])

    # increment and show where we are
    curstep += 1
    test_info()

def step(n):
    pass

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):

    global curstep, curtest, _teststeps

    vvtest = ioninit.cont_args.get('vvtest', None)
    assert vvtest

    # construct an instance of the test class, setup and init it
    vvtmod, vvtclass = vvtest.rsplit('.', 1)
    vandvmod = __import__(vvtmod, fromlist=[vvtclass])

    klass = getattr(vandvmod, vvtclass)
    curtest = klass()

    if hasattr(curtest, 'setup'):
        yield defer.maybeDeferred(curtest.setup)

    # pull out test steps
    rtestmethod = re.compile(r'^s\d+_')
    _teststeps = inspect.getmembers(curtest, lambda x: inspect.ismethod(x) and rtestmethod.match(x.im_func.func_name))

    # add items to 
    control.add_term_name('test_info', test_info)
    control.add_term_name('next', next)
    control.add_term_name('step', step)

    control.add_term_name('curtest', curtest)
    control.add_term_name('curstep', curstep)   # likely unneeded to be public


    print "\n\nV+V app\n\nMethods available: test_info(), next(), step(n)\nVars available: curtest, curstep\n\n"

    reactor.callLater(1, test_info)

    defer.returnValue(('nosupid', 'nostate'))

def stop(container, state):
    pass

