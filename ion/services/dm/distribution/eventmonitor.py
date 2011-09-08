#!/usr/bin/env python

"""
@file ion/services/dm/distribution/eventmonitor.py
@author Dave Foster <dfoster@asascience.com>
@brief Event Monitoring Service
"""

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from twisted.internet import defer
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory
from ion.services.dm.distribution.publisher_subscriber import SubscriberFactory
from ion.services.dm.distribution.events import EventSubscriber
from uuid import uuid4
import time
from ion.core.object.codec import ObjectCodecInterceptor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

EVENTS_EXCHANGE_POINT="events.topic"

EVENTMONITOR_SUBSCRIBE_MESSAGE_TYPE     = object_utils.create_type_identifier(object_id=2335, version=1)
EVENTMONITOR_SUBSCRIBE_RESPONSE_TYPE    = object_utils.create_type_identifier(object_id=2336, version=1)
EVENTMONITOR_UNSUBSCRIBE_MESSAGE_TYPE   = object_utils.create_type_identifier(object_id=2337, version=1)
EVENTMONITOR_GETDATA_MESSAGE_TYPE       = object_utils.create_type_identifier(object_id=2338, version=1)
EVENTMONITOR_DATA_MESSAGE_TYPE          = object_utils.create_type_identifier(object_id=2339, version=1)
EVENTMONITOR_SUBDATA_TYPE               = object_utils.create_type_identifier(object_id=2340, version=1)

class EventMonitorService(ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare(name='event_monitor',
                                             version='0.1.0',
                                             dependencies=["pubsub"])
    class FakeInvocation(object):
        """
        This object is solely for manipulating our received event to stuff into the ObjectCodecInterceptor.
        """
        def __init__(self, msg):
            self.message = msg
            self.content = msg

    def slc_init(self, *args, **kwargs):
        self._subs = {}
        self._subfactory = SubscriberFactory(process=self) #, handler=self._handle_msg)
        self._mc = MessageClient(proc=self)
        self._codec = ObjectCodecInterceptor("fakecodec")
        ServiceProcess.slc_init(self, *args, **kwargs)

    def _handle_msg(self, session_id, subid, msg):
        log.debug("message for you sir %s %s %s" % (session_id, subid, str(msg['content'].datetime)))
        assert self._subs.has_key(session_id) and self._subs[session_id]['subscribers'].has_key(subid)

        # save off datetime so we can filter without having to unpack
        msg['_datetime'] = msg['content'].datetime

        # pack it up as if we were messaging!
        fo = self.FakeInvocation(msg)
        self._codec.after(fo)
        msg = fo.message

        self._subs[session_id]['subscribers'][subid]['msgs'].append(msg)

    def _bump_timestamp(self, session_id):
        assert self._subs.has_key(session_id)
        curtime = time.time()
        self._subs[session_id]['last_request_time'] = curtime
        return curtime

    @defer.inlineCallbacks
    def op_subscribe(self, content, headers, msg):
        """
        Requests a new subscription.
        Expects the content to be an EVENTMONITOR_SUBSCRIBE_MESSAGE_TYPE with a session_id and information about the subscription.
        """
        log.info("op_subscribe")

        # extract message contents
        session_id      = content.session_id
        #subscriber_type = eval(content.subscriber_type) # TODO: safer plz
        event_id        = content.event_id
        origin          = content.origin

        # create new subscription id
        subid           = str(uuid4())[:6]

        # create the subscriber
        sub = yield self._subfactory.build(subscriber_type=EventSubscriber,
                                           event_id=event_id,
                                           origin=origin,
                                           handler=lambda m: self._handle_msg(session_id, subid, m))

        # store this subscriber locally (TODO: for now)
        if not self._subs.has_key(session_id):
            self._subs[session_id] = { 'last_request_time' : '',
                                       'subscribers' : {} }

        self._subs[session_id]['subscribers'][subid] = { 'subscriber': sub, 'msgs': [] }
        self._bump_timestamp(session_id)

        # generate response
        response = yield self._mc.create_instance(EVENTMONITOR_SUBSCRIBE_RESPONSE_TYPE)
        response.session_id = session_id
        response.subscription_id = subid

        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_unsubscribe(self, content, headers, msg):
        """
        Terminates an existing subscription.
        Requires a session_id, and if no subscription_id is specified, will remove all subscribers for that session_id.
        """

        # extract message contents
        session_id      = content.session_id
        subscription_id = content.subscription_id

        # try to look it up
        termsubs = []
        if self._subs.has_key(session_id):
            if subscription_id is None:
                termsubs.extend([y['subscriber'] for y in [x for x in self._subs[session_id]['subscribers'].values()]])
                del(self._subs[session_id])
            else:
                if self._subs[session_id]['subscribers'].has_key(subscription_id):
                    termsubs.append(self._subs[session_id]['subscribers'][subscription_id]['subscriber'])
                    del(self._subs[session_id]['subscribers'][subscription_id])

        # terminate collected active subscribers
        for sub in termsubs:
            log.debug("Unsubscribing from session_id: %s, subscription_id: %s", session_id, subscription_id)
            sub.terminate()

        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_getdata(self, content, headers, msg):
        # extract possible message contents
        session_id      = content.session_id
        timestamp       = content.timestamp
        subscriber_ids  = content.subscriber_id  # may be empty

        # generate response. We give back a nearly empty response if the session id does not exist
        response = yield self._mc.create_instance(EVENTMONITOR_DATA_MESSAGE_TYPE)
        response.session_id = session_id

        if self._subs.has_key(session_id):
            if not timestamp or len(timestamp) == 0:
                timestamp = self._subs[session_id]['last_request_time']
                self._bump_timestamp(session_id)

            try:
                timestamp = float(timestamp)
            except:
                timestamp = 0.0

            log.debug("get_data(): filtering against timestamp [%s]" % str(timestamp))

            for subid, subdata in self._subs[session_id]['subscribers'].iteritems():
                # skip if we have a list of sub ids to give back and this subid is not in the list
                if len(subscriber_ids) > 0 and not subid in subscriber_ids:
                    continue
                dataobj = response.data.add()
                dataobj.subscription_id = subid
                dataobj.subscription_desc = subdata['subscriber']._binding_key #"none for now"

                for idx, event in enumerate([ev for ev in subdata['msgs'] if ev['_datetime'] >= timestamp]):

                    # unpack it up as if we were messaging!
                    fo = self.FakeInvocation(event.copy())
                    self._codec.before(fo)
                    event = fo.content
                    self.workbench.put_repository(event['content'].Repository)

                    link = dataobj.events.add()
                    link.SetLink(event['content'].MessageObject)
        yield self.reply_ok(msg, response)

class EventMonitorServiceClient(ServiceClient):

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "event_monitor"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def subscribe(self, msg):
        yield self._check_init()

        (content, headers, rmsg) = yield self.rpc_send('subscribe', msg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def unsubscribe(self, msg):
        yield self._check_init()

        (content, headers, rmsg) = yield self.rpc_send('unsubscribe', msg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def getdata(self, msg):
        yield self._check_init()

        (content, headers, rmsg) = yield self.rpc_send('getdata', msg)
        defer.returnValue(content)

factory = ProcessFactory(EventMonitorService)
