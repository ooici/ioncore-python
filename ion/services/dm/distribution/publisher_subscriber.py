#!/usr/bin/env python

"""
@file ion/services/dm/distribution/publisher_subscriber.py
@author Paul Hubbard
@author Dave Foster <dfoster@asascience.com>
@brief Publisher/Subscriber classes for attaching to processes
"""

from ion.core.process.process import ProcessClientBase
from ion.core.messaging.receiver import PublisherReceiver, SubscriberReceiver

class Publisher(ProcessClientBase):
    """
    @brief This represents publishers of (mostly) science data. Intended use is
    to be instantiated within another class/process/codebase, as an object for sending data to OOI.
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    """

    def __init__(self, proc=None):
        """
        Save class variables for later
        """
        ProcessClientBase.__init__(self, proc)
        self._resource_id = ''
        self._exchange_point = ''

        self._publish_receiver = PublisherReceiver(self.proc.id.full) # TODO: does it matter?

        # do NOT attach the receiver - don't even init it. We aren't creating queues with it.

    def register(self, xp_name, publisher_name, credentials):
        """
        @brief Register a new publisher, also does the access control step.
        @param xp_name Name of exchange point to use
        @param publisher_name Name of new publisher process, free-form string
        @param credentials Placeholder for auth* tokens
        @retval OK or error
        @note saves resource id to self._resource_id
        """
        pass

    def unregister(self):
        """
        @brief Remove a registration when done
        @note Uses class variables self._resource_id and self._exchange_point
        @retval Return code only
        """
        pass

    def publish(self, topic, data):
        """
        @brief Publish data on a specified resource id/topic
        @param topic Topic to send to, in form of dataset.variable
        @param data Data, OOI-format, protocol-buffer encoded
        @retval Deferred on send, not RPC
        """
        return self._publish_receiver.send(exchange_point=self._exchange_point, topic=topic, resource_id=self._resource_id, data=data)

class Subscriber(ProcessClientBase):
    """
    @brief This represents subscribers, both user-driven and internal (e.g. dataset persister)
    @note All returns are HTTP return codes, 2xx for success, etc, unless otherwise noted.
    @todo Need a subscriber receiver that can hook into the topic xchg mechanism
    """
    def __init__(self, proc=None):
        """
        Save class variables for later
        """
        ProcessClientBase.__init__(self, proc)
        self._resource_id = ''
        self._exchange_point = ''

        self._subscribe_receiver = SubscriberReceiver(self.proc.id.full, handler=self._receive_handler) # TODO: use what for name here?

    def subscribe(self, xp_name, topic_regex):
        """
        @brief Register a topic to subscribe to. Results will be sent to our private queue.
        @note Allow third-party subscriptions?
        @note Allow multiple subscriptions?
        @param xp_name Name of exchange point to use
        @param topic_regex Dataset topic of interest, using amqp regex
        @retval Return code only, with new queue automagically hooked up to ondata()?
        """
        pass

    def unsubscribe(self):
        """
        @brief Remove a subscription
        @retval Return code only
        """
        pass

    def _receive_handler(self, data, msg):
        """
        Default handler for messages received by the SubscriberReceiver.
        Acks the message and calls the ondata handler.
        @param data Data packet/message matching subscription
        @param msg  Message instance
        @return The return value of ondata.
        """
        msg.ack()
        return self.ondata(data)

    def ondata(self, data):
        """
        @brief Data callback, in the pattern of the current subscriber code
        @param data Data packet/message matching subscription
        @retval None, may daisy chain output back into messaging system
        """
        raise NotImplemented('Must be implmented by subclass')

