#!/usr/bin/env python

"""
@file ion/zapps/association.py
@author Matt Rodriguez
@brief simple app that tests the performance of the message stack
"""
import time
from twisted.internet import defer

from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc, Process
from ion.core.cc.shell import control

from ion.play.hello_service import HelloServiceClient


from ion.core import ioninit
CONF = ioninit.config(__name__)

@defer.inlineCallbacks
def send_messages():
    proc = Process()
    yield proc.spawn()
    hc = HelloServiceClient(proc)

    count = 600
    tzero = time.time()
    
    yield hc._check_init()

    d = []
    for x in xrange(count):
        d.append(hc.hello_deferred("Hi there, hello1"))
        print x

    yield defer.DeferredList(d)
    print 'done'

    delta_t = (time.time() - tzero)
    print('%f elapsed, %f per second' % (delta_t, float(count) / delta_t) )
    defer.returnValue(None)
    
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    
    yield "I do nothing"
    control.add_term_name('send_messages',send_messages)
    #res = (supid.full, [appsup_desc])
    defer.returnValue(["nopid",[]])
    
@defer.inlineCallbacks
def stop(container, state):
    
    supdesc = state[0]
    yield supdesc.terminate()