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

EVENT_MESSAGE_TYPE                          = object_utils.create_type_identifier(object_id=2322, version=1)
RESOURCE_LIFECYCLE_EVENT_MESSAGE_TYPE       = object_utils.create_type_identifier(object_id=2323, version=1)
TRIGGER_EVENT_MESSAGE_TYPE                  = object_utils.create_type_identifier(object_id=2324, version=1)
RESOURCE_MODIFICATION_EVENT_MESSAGE_TYPE    = object_utils.create_type_identifier(object_id=2325, version=1)
LOGGING_EVENT_MESSAGE_TYPE                  = object_utils.create_type_identifier(object_id=2326, version=1)
