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
RESOURCE_LIFECYCLE_EVENT_ID = 1001
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

        # copy kwargs into local list
        msgargs = kwargs.copy()

        # create base event message, assign values from kwargs
        event_msg = yield self._mc.create_instance(EVENT_MESSAGE_TYPE)
        for k,v in msgargs.items():
            if hasattr(event_msg, k):
                setattr(event_msg, k, v)
                msgargs.pop(k)

        # create additional event msg (specific to this event notification), assign values from kwargs
        additional_event_msg = event_msg.CreateObject(self.msg_type)
        for k, v in msgargs.items():
            if hasattr(additional_event_msg, k):
                setattr(additional_event_msg, k, v)
                msgargs.pop(k)

        # error checking: see if we have any remaining kwargs
        if len(msgargs) > 0:
            raise Exception("create_event: unused kwargs remaining (%s), unused by base event message and msg type id %s" % (str(msgargs), str(self.msg_type)))

        # TODO: perhaps check to make sure additional_event_msg and event_msg have exclusive attribute names, or we'll assign to one and not the other

        # link them
        event_msg.additional_data = additional_event_msg

        defer.returnValue(event_msg)

    @defer.inlineCallbacks
    def publish_event(self, event_msg, origin=None, **kwargs):
        origin = origin or self._origin
        assert origin and origin != "unknown"

        yield self.publish(event_msg, routing_key=self.topic(origin))

    @defer.inlineCallbacks
    def create_and_publish_event(self, **kwargs):
        """
        """
        msg = yield self.create_event(**kwargs)
        yield self.publish_event(msg, origin=kwargs.get('origin', None))

class ResourceLifecycleEventPublisher(EventPublisher):
    """
    Event Notification Publisher for Resource lifecycle events. Used as a concrete derived class, and as a base for
    specializations such as ContainerLifecycleEvents and ProcessLifecycleEvents.

    The "origin" parameter in this class' initializer should be the resource id (UUID).
    """

    msg_type = RESOURCE_LIFECYCLE_EVENT_MESSAGE_TYPE
    event_id = RESOURCE_LIFECYCLE_EVENT_ID

class ContainerLifecycleEventPublisher(ResourceLifecycleEventPublisher):
    """
    Event Notification Publisher for Container lifecycle events.

    The "origin" parameter in this class' initializer should be the container name.
    """
    event_id = CONTAINER_LIFECYCLE_EVENT_ID

class ProcessLifecycleEventPublisher(ResourceLifecycleEventPublisher):
    """
    Event Notification Publisher for Process lifecycle events.

    The "origin" parameter in this class' initializer should be the process' exchange name.
    """
    event_id = PROCESS_LIFECYCLE_EVENT_ID

class TriggerEventPublisher(EventPublisher):
    """
    Base Publisher class for "triggered" Event Notifications.
    """
    msg_type = TRIGGER_EVENT_MESSAGE_TYPE

class DatasourceUpdateEventPublisher(TriggerEventPublisher):
    """
    Event Notification Publisher for Datasource updates.

    The "origin" parameter in this class' initializer should be the datasource resource id (UUID).
    """
    event_id = DATASOURCE_UPDATE_EVENT_ID

class ResourceModifiedEventPublisher(EventPublisher):
    """
    Base Publisher class for resource modification Event Notifications. This is distinct from resource lifecycle state
    Event Notifications.
    """
    msg_type = RESOURCE_MODIFICATION_EVENT_MESSAGE_TYPE

class DatasetModificationEventPublisher(ResourceModifiedEventPublisher):
    """
    Event Notification Publisher for Dataset Modifications.

    The "origin" parameter in this class' initializer should be the dataset resource id (UUID).
    """
    event_id = DATASET_MODIFICATION_EVENT_ID

class ScheduleEventPublisher(TriggerEventPublisher):
    """
    Event Notification Publisher for Scheduled events (ie from the Scheduler service).
    """
    event_id = SCHEDULE_EVENT_ID

class LoggingEventPublisher(EventPublisher):
    """
    Base Publisher for logging Event Notifications.
    """
    msg_type = LOGGING_EVENT_MESSAGE_TYPE

class CriticalLoggingEventPublisher(LoggingEventPublisher):
    """
    Event Notification Publisher for critical logging events.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = LOGGING_CRITICAL_EVENT_ID

class ErrorLoggingEventPublisher(LoggingEventPublisher):
    """
    Event Notification Publisher for error logging events.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = LOGGING_ERROR_EVENT_ID

#
#
# ################################################################################
#
#

class EventSubscriber(Subscriber):
    """
    Base Subscriber for event notifications.

    This base subscriber is capable of subscribing to any event notification type with any origin. You must assign
    the on_data callable in your instance (to a callable taking a data param) or use a SubscriberFactory with
    the subscriber_type set to this class and specify your handler to its build method.
    """

    event_id = None

    def topic(self, origin):
        """
        Builds the topic that this event should be published to.
        If either side of the event_id.origin pair are missing, will subscribe to anything.
        """
        event_id = self.event_id or "*"
        origin = origin or "*"

        return "%s.%s" % (str(event_id), str(origin))

    def __init__(self, xp_name=None, binding_key=None, queue_name=None, credentials=None, process=None, event_id=None, origin=None, *args, **kwargs):
        """
        Initializer.

        You may wish to set either event_id or origin. A normal SubscriberFactory, with this class specified
        as the subscriber_type, will pass event_id or origin as kwargs here when specified to the build 
        method.
        """
        # only set this if the user specified something to this initializer and there's no class default
        if self.event_id is None and not event_id is None:
            self.event_id = event_id

        xp_name = xp_name or EVENTS_EXCHANGE_POINT
        binding_key = binding_key or self.topic(origin)

        Subscriber.__init__(self, xp_name=xp_name, binding_key=binding_key, queue_name=queue_name, credentials=credentials, process=process, *args, **kwargs)

