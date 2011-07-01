#!/usr/bin/env python

"""
@file ion/vandv/vandvapp.py
@author Dave Foster <dfoster@asascience.com>
@brief Helper starter for v+v tests.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

#from ion.core.ioninit import ion_config
from ion.core import ioninit
from ion.core.cc.shell import control
from ion.util.procutils import get_class

#-- CC Application interface

# global fun!
curtest = None
curstep = None

def test_info():
    pass

def next():
    pass

def step(n):
    pass

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):

    vvtest = ioninit.cont_args.get('vvtest', None)
    assert vvtest

    # construct an instance of the test class, setup and init it
    print vvtest
    klass = get_class(vvtest)
    print klass
    curtest = klass()

    yield defer.maybeDeferred(curtest.setup)

    # add items to 
    control.add_term_name('test_info', test_info)
    control.add_term_name('next', next)
    control.add_term_name('step', step)

    control.add_term_name('curtest', curtest)
    control.add_term_name('curstep', curstep)   # likely unneeded to be public


    print "V+V app\n\nMethods available: test_info(), next(), step(n)\nVars available: curtest, curstep\n\n"

    defer.returnValue(True)

def stop(container, state):
    pass

