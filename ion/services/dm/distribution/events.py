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
from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import time

def get_events_exchange_point():
    return "%s.events.topic" % ioninit.sys_name

EVENT_MESSAGE_TYPE                          = object_utils.create_type_identifier(object_id=2322, version=1)
RESOURCE_LIFECYCLE_EVENT_MESSAGE_TYPE       = object_utils.create_type_identifier(object_id=2323, version=1)
TRIGGER_EVENT_MESSAGE_TYPE                  = object_utils.create_type_identifier(object_id=2324, version=1)
RESOURCE_MODIFICATION_EVENT_MESSAGE_TYPE    = object_utils.create_type_identifier(object_id=2325, version=1)
LOGGING_EVENT_MESSAGE_TYPE                  = object_utils.create_type_identifier(object_id=2326, version=1)
NEW_SUBSCRIPTION_EVENT_MESSAGE_TYPE         = object_utils.create_type_identifier(object_id=2327, version=1)
DEL_SUBSCRIPTION_EVENT_MESSAGE_TYPE         = object_utils.create_type_identifier(object_id=2328, version=1)
DATA_EVENT_MESSAGE_TYPE                     = object_utils.create_type_identifier(object_id=2329, version=1)
DATASOURCE_UNAVAILABLE_EVENT_MESSAGE_TYPE   = object_utils.create_type_identifier(object_id=2330, version=1)
DATASET_SUPPLEMENT_ADDED_EVENT_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=2331, version=1)
INSTRUMENT_SAMPLE_DATA_EVENT_MESSAGE_TYPE   = object_utils.create_type_identifier(object_id=4303, version=1)

# event IDs: https://confluence.oceanobservatories.org/display/syseng/CIAD+DM+SV+Notifications+and+Events
RESOURCE_LIFECYCLE_EVENT_ID = 1001
CONTAINER_LIFECYCLE_EVENT_ID = 1051
PROCESS_LIFECYCLE_EVENT_ID = 1052
DATASOURCE_UPDATE_EVENT_ID = 1101
DATASOURCE_UNAVAILABLE_EVENT_ID = 1102
DATASET_SUPPLEMENT_ADDED_EVENT_ID = 1111
BUSINESS_STATE_MODIFICATION_EVENT_ID = 1112
NEW_SUBSCRIPTION_EVENT_ID = 1201
DEL_SUBSCRIPTION_EVENT_ID = 1202
SCHEDULE_EVENT_ID = 2001
LOGGING_INFO_EVENT_ID = 3003
LOGGING_ERROR_EVENT_ID = 3002
LOGGING_CRITICAL_EVENT_ID = 3001
DATABLOCK_EVENT_ID = 4001


class EventPublisher(Publisher):
    """
    Base publisher for Event Notifications.

    Events are published by various items in the ION system with no knowledge or requirement that anything
    be listening to them. The EventPublisher base class defines a specially derived Publisher class that
    can be used to create and publish event notification messages. By default, events are published to the
    exchange point 'events.topic'.

    You should not use an instance of EventPublisher directly, rather, use one of its derived implementations.

    Call create_event and then publish_event (or the combination method, create_and_publish_event) to send
    an event notification into the system.

    The create_event method takes kwargs which set the message fields. This is meant to be used in a convenience
    fashion, you may still alter the message create_event returns using normal message semantics.

    The message sent by EventPublisher is the basic EventMessage (id 2322) which contains common information
    about an event, and the additional_data field is defined as another message, specific to the type of event
    being published.

    Implementers:
        - Override msg_type and event_id in your derived classes. The event_id is unique to each type of event
          notification, and the msg_type is the type of message that will occupy the additional_data field.
        - If your message contains any enum fields, you should define convienence classes to allow the user to
          set the value of those enum fields in create_event without needing to have an instance of the
          message ahead of time. These classes should follow this model:
              # enum Direction for 'direction' field in message
              class Direction:
                  NORTH = 'NORTH'
                  SOUTH = 'SOUTH'
                  EAST  = 'EAST'
                  WEST  = 'WEST'

          Then, when calling create_event, you can use:
              SomePublisher.create_event(direction=SomePublisher.Direction.EAST)

          Alternatly, you may set the field in two steps:
              msg = yield SomePublisher.create_event()
              msg.direction = msg.Direction.EAST        # using the enum as defined in the message
    """

    msg_type = None
    event_id = None

    class Status:
        """
        The enum Status as defined in the EventMessage object.
        Use this to set the 'status' field using create_event.
        """
        IN_PROGRESS = 'IN_PROGRESS'
        CACHED      = 'CACHED'
        ERROR       = 'ERROR'
        NO_CACHE    = 'NO_CACHE'

    def topic(self, origin):
        """
        Builds the topic that this event should be published to.
        """
        assert self.event_id and origin
        
        return "%s.%s" % (str(self.event_id), str(origin))
        
    def __init__(self, xp_name=None, routing_key=None, process=None, origin="unknown", *args, **kwargs):
        """
        Initializer override.
        Sets defaults for the EventPublisher.

        @param origin   Sets the origin used in the topic when publishing the event.
                        This can be overridden when calling publish.
        """
        self._origin = origin
        self._mc = MessageClient(proc=process)

        xp_name = xp_name or get_events_exchange_point()
        routing_key = routing_key or "unknown"

        Publisher.__init__(self, xp_name=xp_name, routing_key=routing_key, process=process, *args, **kwargs)

    def _set_msg_fields(self, msg, msgargs):
        """
        Helper method to set fields of a Message instance. Used by create_event.

        @param msg      The Message instance to set fields on.
        @param msgargs  The dict of field -> values to set fields on the msg with. As fields are found in
                        the message and set, they are removed from the msgarms param. Passed by ref from
                        create_event. When an Enum field is discovered that we are trying to set, this method
                        attempts to find the real enum value by the name of the enum member passed in.
        """
        for k,v in msgargs.items():
            if hasattr(msg, k):
                log.debug("_set_msg_fields: setting field %s" % k)
                # is this an enum field and we've passed a string that looks like it could be a name?
                if msg._Properties[k].field_type == "TYPE_ENUM" and isinstance(v, str):
                    # translate v into the real value
                    assert hasattr(msg._Properties[k].field_enum, v)
                    v = getattr(msg._Properties[k].field_enum, v)
                    log.debug("Setting enum field %s to %s" % (str(k), str(v)))

                setattr(msg, k, v)
                msgargs.pop(k)

    @defer.inlineCallbacks
    def create_event(self, **kwargs):
        """
        Creates an EVENT_MESSAGE_TYPE with the additional_data field pointing to a msg of type "msg_type" as defined
        by this EventPublisher's derivation.

        @returns The event message.
        """
        assert self.msg_type

        if not kwargs.has_key('datetime'):
            log.warn("Automatically setting 'datetime' field")
            kwargs['datetime'] = time.time()

        # copy kwargs into local list
        msgargs = kwargs.copy()
        log.warn("create_event has %d kwargs to set" % len(msgargs))

        # create base event message, assign values from kwargs
        event_msg = yield self._mc.create_instance(EVENT_MESSAGE_TYPE)
        self._set_msg_fields(event_msg, msgargs)

        # create additional event msg (specific to this event notification), assign values from kwargs
        additional_event_msg = event_msg.CreateObject(self.msg_type)
        self._set_msg_fields(additional_event_msg, msgargs)

        # error checking: see if we have any remaining kwargs
        if len(msgargs) > 0:
            raise Exception("create_event: unused kwargs remaining (%s), unused by base event message and msg type id %s" % (str(msgargs), str(self.msg_type)))

        # TODO: perhaps check to make sure additional_event_msg and event_msg have exclusive attribute names, or we'll assign to one and not the other

        # link them
        event_msg.additional_data = additional_event_msg
        defer.returnValue(event_msg)

    @defer.inlineCallbacks
    def publish_event(self, event_msg, origin=None, **kwargs):
        """
        Publishes an event notification.

        @param event_msg    The event message to publish.
        @param origin       The origin to use in the topic. If not set, uses the origin set in the initializer.
        """
        origin = origin or self._origin
        assert origin and origin != "unknown"

        routing_key=self.topic(origin)
        log.debug("Publishing message to %s" % routing_key)

        yield self.publish(event_msg, routing_key=routing_key)

    @defer.inlineCallbacks
    def create_and_publish_event(self, **kwargs):
        """
        Convenience method which calls both create_event and publish_event in one shot.
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

    # enum for State
    class State:
        NEW           = 'NEW'
        READY         = 'READY'
        ACTIVE        = 'ACTIVE'
        TERMINATED    = 'TERMINATED'
        ERROR         = 'ERROR'

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

class DatasourceUnavailableEventPublisher(ResourceModifiedEventPublisher):
    """
    Event Notification Publisher for the Datasource Unavailable event.
    """
    event_id = DATASOURCE_UNAVAILABLE_EVENT_ID
    msg_type = DATASOURCE_UNAVAILABLE_EVENT_MESSAGE_TYPE

class DatasetSupplementAddedEventPublisher(ResourceModifiedEventPublisher):
    """
    Event Notification Publisher for Dataset Supplement Added.

    The "origin" parameter in this class' initializer should be the dataset resource id (UUID).
    """
    event_id = DATASET_SUPPLEMENT_ADDED_EVENT_ID
    msg_type = DATASET_SUPPLEMENT_ADDED_EVENT_MESSAGE_TYPE

class BusinessStateModificationEventPublisher(ResourceModifiedEventPublisher):
    """
    Event Notification Publisher for Dataset Modifications.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = BUSINESS_STATE_MODIFICATION_EVENT_ID
    
class NewSubscriptionEventPublisher(EventPublisher):
    """
    Event Notification Publisher for Subscription Modifications.

    The "origin" parameter in this class' initializer should be the dispatcher resource id (UUID).
    """
    msg_type = NEW_SUBSCRIPTION_EVENT_MESSAGE_TYPE
    event_id = NEW_SUBSCRIPTION_EVENT_ID

class DelSubscriptionEventPublisher(EventPublisher):
    """
    Event Notification Publisher for Subscription Modifications.

    The "origin" parameter in this class' initializer should be the dispatcher resource id (UUID).
    """
    msg_type = DEL_SUBSCRIPTION_EVENT_MESSAGE_TYPE
    event_id = DEL_SUBSCRIPTION_EVENT_ID

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

class InfoLoggingEventPublisher(LoggingEventPublisher):
    """
    Event Notification Publisher for informational logging events.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = LOGGING_INFO_EVENT_ID

class DataEventPublisher(EventPublisher):
    """
    Event Notification Publisher for Subscription Modifications.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    msg_type = DATA_EVENT_MESSAGE_TYPE

class DataBlockEventPublisher(DataEventPublisher):
    """
    Event Notification Publisher for Subscription Modifications.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = DATABLOCK_EVENT_ID
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
        event_id = self._event_id or "*"
        origin = origin or "#"
        
        return "%s.%s" % (str(event_id), str(origin))

    def __init__(self, xp_name=None, binding_key=None, event_id=None, origin=None, *args, **kwargs):
        """
        Initializer.

        You may wish to set either event_id or origin. A normal SubscriberFactory, with this class specified
        as the subscriber_type, will pass event_id or origin as kwargs here when specified to the build 
        method.
        """
        self._event_id = event_id or self.event_id

        xp_name = xp_name or get_events_exchange_point()
        binding_key = binding_key or self.topic(origin)

        Subscriber.__init__(self, xp_name=xp_name, binding_key=binding_key, *args, **kwargs)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        log.debug("Listening to events on %s" % self._binding_key)
        yield Subscriber.on_activate(self, *args, **kwargs)

class ResourceLifecycleEventSubscriber(EventSubscriber):
    """
    Event Notification Subscriber for Resource lifecycle events. Used as a concrete derived class, and as a base for
    specializations such as ContainerLifecycleEvents and ProcessLifecycleEvents.

    The "origin" parameter in this class' initializer should be the resource id (UUID).
    """
    event_id = RESOURCE_LIFECYCLE_EVENT_ID

class ContainerLifecycleEventSubscriber(ResourceLifecycleEventSubscriber):
    """
    Event Notification Subscriber for Container lifecycle events.

    The "origin" parameter in this class' initializer should be the container name.
    """
    event_id = CONTAINER_LIFECYCLE_EVENT_ID

class ProcessLifecycleEventSubscriber(ResourceLifecycleEventSubscriber):
    """
    Event Notification Subscriber for Process lifecycle events.

    The "origin" parameter in this class' initializer should be the process' exchange name.
    """
    event_id = PROCESS_LIFECYCLE_EVENT_ID

class TriggerEventSubscriber(EventSubscriber):
    """
    Base Subscriber class for "triggered" Event Notifications.
    """
    pass

class DatasourceUpdateEventSubscriber(TriggerEventSubscriber):
    """
    Event Notification Subscriber for Datasource updates.

    The "origin" parameter in this class' initializer should be the datasource resource id (UUID).
    """
    event_id = DATASOURCE_UPDATE_EVENT_ID

class ResourceModifiedEventSubscriber(EventSubscriber):
    """
    Base Subscriber class for resource modification Event Notifications. This is distinct from resource lifecycle state
    Event Notifications.
    """
    pass

class DatasourceUnavailableEventSubscriber(ResourceModifiedEventSubscriber):
    """
    Event Notification Subscriber for the Datasource Unavailable event.

    The "origin" parameter in this class' initializer should be the datasource resource id (UUID).
    """
    event_id = DATASOURCE_UNAVAILABLE_EVENT_ID

class DatasetSupplementAddedEventSubscriber(ResourceModifiedEventSubscriber):
    """
    Event Notification Subscriber for Dataset Supplement Added.

    The "origin" parameter in this class' initializer should be the dataset resource id (UUID).
    """
    event_id = DATASET_SUPPLEMENT_ADDED_EVENT_ID

class BusinessStateChangeSubscriber(ResourceModifiedEventSubscriber):
    """
    Event Notification Subscriber for Data Block changes.

    The "origin" parameter in this class' initializer should be the process' exchagne name (TODO: correct?)
    """
    event_id = BUSINESS_STATE_MODIFICATION_EVENT_ID
    
class NewSubscriptionEventSubscriber(EventSubscriber):
    """
    Event Notification Subscriber for Subscription Modifications.

    The "origin" parameter in this class' initializer should be the dispatcher resource id (UUID).
    """
    event_id = NEW_SUBSCRIPTION_EVENT_ID

class DelSubscriptionEventSubscriber(EventSubscriber):
    """
    Event Notification Subscriber for Subscription Modifications.

    The "origin" parameter in this class' initializer should be the dispatcher resource id (UUID).
    """
    event_id = DEL_SUBSCRIPTION_EVENT_ID

class ScheduleEventSubscriber(TriggerEventSubscriber):
    """
    Event Notification Subscriber for Scheduled events (ie from the Scheduler service).
    """
    event_id = SCHEDULE_EVENT_ID

class LoggingEventSubscriber(EventSubscriber):
    """
    Base Subscriber for logging Event Notifications.
    """
    pass

class CriticalLoggingEventSubscriber(LoggingEventSubscriber):
    """
    Event Notification Subscriber for critical logging events.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = LOGGING_CRITICAL_EVENT_ID

class ErrorLoggingEventSubscriber(LoggingEventSubscriber):
    """
    Event Notification Subscriber for error logging events.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = LOGGING_ERROR_EVENT_ID

class InfoLoggingEventSubscriber(LoggingEventSubscriber):
    """
    Event Notification Subscriber for informational logging events.

    The "origin" parameter in this class' initializer should be the process' exchange name (TODO: correct?)
    """
    event_id = LOGGING_INFO_EVENT_ID
    
class DataEventSubscriber(EventSubscriber):
    """
    Event Notification Subscriber for Data Block changes.

    The "origin" parameter in this class' initializer should be the process' exchagne name (TODO: correct?)
    """
    pass

class DataBlockEventSubscriber(DataEventSubscriber):
    """
    Event Notification Subscriber for Data Block changes.

    The "origin" parameter in this class' initializer should be the process' exchagne name (TODO: correct?)
    """
    event_id = DATABLOCK_EVENT_ID

