#!/usr/bin/env python

"""
@file res/scripts/notifytest.py
@author Dave Foster <dfoster@asascience.com>
@brief pubsub notification tests
"""
from twisted.internet import defer, reactor

try:
    import json
except:
    import simplejson as json

from ion.core import ioninit
from ion.core import bootstrap
from ion.core.process.process import Process

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
EVENTMONITOR_SUBSCRIBE_MESSAGE_TYPE     = object_utils.create_type_identifier(object_id=2335, version=1)
EVENTMONITOR_UNSUBSCRIBE_MESSAGE_TYPE   = object_utils.create_type_identifier(object_id=2337, version=1)
EVENTMONITOR_GETDATA_MESSAGE_TYPE       = object_utils.create_type_identifier(object_id=2338, version=1)
EVENTMONITOR_DATA_MESSAGE_TYPE          = object_utils.create_type_identifier(object_id=2339, version=1)

from ion.services.dm.distribution.events import RESOURCE_LIFECYCLE_EVENT_ID, ResourceLifecycleEventPublisher, ResourceLifecycleEventSubscriber, \
DATABLOCK_EVENT_ID,  DataBlockEventPublisher, DataBlockEventSubscriber
from ion.services.dm.distribution.eventmonitor import EventMonitorServiceClient

callbackid = None

@defer.inlineCallbacks
def publish(proc):

    rep = DataBlockEventPublisher(process=proc)
    yield rep.initialize()
    yield rep.activate()
    # TODO: register

    yield rep.create_and_publish_event(origin="Tom", data_block = json.dumps([{'temperature': -0.88160000000000005, 'salinity': 20.105599999999999, 'sound_velocity': 1424.9690000000001, 'pressure': -3.3119999999999998, 'time': '2011-05-18T15:32:36', 'conductivity': 1.70343}]))

    global callbackid
    callbackid = reactor.callLater(5, publish, proc)

@defer.inlineCallbacks
def getcount(mc, ec):
    data = yield get_data(mc, ec)

    print "MESSAGES : %d" % len(data.data[0].events)

def stop_publishing():
    global callbackid
    callbackid.cancel()
    #reactor.cancelCallLater(callbackid)
    print "stopped!"

@defer.inlineCallbacks
def get_data(mc, ec):
    msg = yield mc.create_instance(EVENTMONITOR_GETDATA_MESSAGE_TYPE)
    msg.session_id="uno"
    #msg.timestamp = ""

    data = yield ec.getdata(msg)
    defer.returnValue(data)

@defer.inlineCallbacks
def makeresp(mc, ec):
    msg = yield mc.create_instance(EVENTMONITOR_DATA_MESSAGE_TYPE)
    msg.session_id="uno"

    defer.returnValue(msg)

@defer.inlineCallbacks
def start():
    """
    Main function of bootstrap.
    """
    proc = Process()
    yield proc.spawn()

    mc = MessageClient(proc=proc)
    msg = yield mc.create_instance(EVENTMONITOR_SUBSCRIBE_MESSAGE_TYPE)

    msg.session_id="dos"
    msg.event_id=DATABLOCK_EVENT_ID
    msg.origin = "*"

    ec = EventMonitorServiceClient()
    resp = yield ec.subscribe(msg)

    print "Set up subscriber", resp.subscription_id

    # start publishing every 5 seconds
    reactor.callLater(5, publish, proc)

    from ion.core.cc.shell import control
    control.add_term_name("mc", mc)
    control.add_term_name("ec", ec)
    control.add_term_name("stop_publishing", stop_publishing)
    control.add_term_name("get_data", get_data)
    control.add_term_name("makeresp", makeresp)
    control.add_term_name("getcount", getcount)

    print "'ec', 'mc', 'stop_publishing()', 'get_data()', 'makeresp()', 'getcount()' available."
    #print "'stop_publishing()' available"

start()

