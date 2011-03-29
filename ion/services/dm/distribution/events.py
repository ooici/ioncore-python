#!/usr/bin/env python

"""
@file ion/services/dm/distribution/events.py
@author Dave Foster <dfoster@asascience.com>
@brief Event notification
"""

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from twisted.internet import defer
from ion.services.dm.distribution.publisher_subscriber import Publisher, Subscriber

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

EVENTS_EXCHANGE_POINT="events.topic"

EVENT_MESSAGE_TYPE                          = object_utils.create_type_identifier(object_id=2322, version=1)
RESOURCE_LIFECYCLE_EVENT_MESSAGE_TYPE       = object_utils.create_type_identifier(object_id=2323, version=1)
TRIGGER_EVENT_MESSAGE_TYPE                  = object_utils.create_type_identifier(object_id=2324, version=1)
RESOURCE_MODIFICATION_EVENT_MESSAGE_TYPE    = object_utils.create_type_identifier(object_id=2325, version=1)
LOGGING_EVENT_MESSAGE_TYPE                  = object_utils.create_type_identifier(object_id=2326, version=1)

# event IDs: https://confluence.oceanobservatories.org/display/syseng/CIAD+DM+SV+Notifications+and+Events
RESOUCE_LIFECYCLE_EVENT_ID = 1001
CONTAINER_LIFECYCLE_EVENT_ID = 1051
PROCESS_LIFECYCLE_EVENT_ID = 1052
DATASOURCE_UPDATE_EVENT_ID = 1101
DATASET_MODIFICATION_EVENT_ID = 1111
SCHEDULE_EVENT_ID = 2001
LOGGING_ERROR_EVENT_ID = 3002
LOGGING_CRITICAL_EVENT_ID = 3001

class EventPublisher(Publisher):
    """
    Base publisher for Event Notifications.

    Override msg_type and event_id in your derived classes.
    """

    msg_type = None
    event_id = None

    def topic(self, origin):
        """
        Builds the topic that this event should be published to.
        """
        assert self.event_id and origin
        return "%s.%s" % (str(self.event_id), str(origin))

    def __init__(self, xp_name=None, routing_key=None, credentials=None, process=None, origin="unknown", *args, **kwargs):
        self._origin = origin
        self._mc = MessageClient(proc=process)

        xp_name = xp_name or EVENTS_EXCHANGE_POINT
        routing_key = routing_key or "unknown"

        Publisher.__init__(self, xp_name=xp_name, routing_key=routing_key, credentials=credentials, process=process, *args, **kwargs)

    @defer.inlineCallbacks
    def create_event(self, **kwargs):
        """
        Creates an EVENT_MESSAGE_TYPE with the additional_data field pointing to a msg of type "msg_type" as defined
        by this EventPublisher's derivation.

        @returns The event message.
        """
        assert self.msg_type
        event_msg = yield self._mc.create_instance(EVENT_MESSAGE_TYPE)
        # TODO: assign common vars from kwargs

        additional_event_msg = event_msg.CreateObject(self.msg_type)
        # TODO: assign specific event msg vars

        # link them
        event_msg.additional_data = additional_event_msg

        defer.returnValue(event_msg)

    @defer.inlineCallbacks
    def publish_event(self, event_msg, origin=None, **kwargs):
        origin = origin or self._origin
        assert origin and origin != "unknown"

        yield self.publish(event_msg, routing_key=self.topic(origin))

    @defer.inlineCallbacks
    def create_and_publish_event(self, origin=None, **kwargs):
        msg = yield self.create_event(**kwargs)
        yield self.publish_event(msg, origin=origin)


