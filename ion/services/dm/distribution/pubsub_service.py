#!/usr/bin/env python

"""
@file ion/services/dm/distribution/pubsub_service.py
@package ion.services.dm.distribution.pubsub
@author Paul Hubbard
@author Michael Meisinger
@author David Stuebe
@brief service for publishing on data streams, and for subscribing to streams.
The service includes methods for defining topics, defining publishers, publishing,
and defining subscriptions.
"""

import time
import re
from twisted.internet import defer

from ion.core.exception import ApplicationError
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core import ioninit
import ion.util.ionlog
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
from ion.services.coi.exchange.exchange_management import ExchangeManagementClient

# Global objects
CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)

# References to protobuf message/object definitions
TOPIC_TYPE = object_utils.create_type_identifier(object_id=2307, version=1)
PUBLISHER_TYPE = object_utils.create_type_identifier(object_id=2310, version=1)
SUBSCRIBER_TYPE = object_utils.create_type_identifier(object_id=2311, version=1)
QUEUE_TYPE = object_utils.create_type_identifier(object_id=2308, version=1)
BINDING_TYPE = object_utils.create_type_identifier(object_id=2314, version=1)

# Generic request and response wrapper message types
REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)

# Query and response types
REGEX_TYPE = object_utils.create_type_identifier(object_id=2306, version=1)
IDLIST_TYPE = object_utils.create_type_identifier(object_id=2312, version=1)
XS_TYPE = object_utils.create_type_identifier(object_id=2313, version=1)
XP_TYPE = object_utils.create_type_identifier(object_id=2309, version=1)

# Resource GPB objects
XS_RES_TYPE = object_utils.create_type_identifier(object_id=2315, version=1)
XP_RES_TYPE = object_utils.create_type_identifier(object_id=2316, version=1)
TOPIC_RES_TYPE = object_utils.create_type_identifier(object_id=2317, version=1)
PUBLISHER_RES_TYPE = object_utils.create_type_identifier(object_id=2318, version=1)
SUBSCRIBER_RES_TYPE = object_utils.create_type_identifier(object_id=2319, version=1)
QUEUE_RES_TYPE = object_utils.create_type_identifier(object_id=2321, version=1)
BINDING_RES_TYPE = object_utils.create_type_identifier(object_id=2320, version=1)

class PSSException(ApplicationError):
    """
    Exception class for the pubsub service.
    """

class PubSubService(ServiceProcess):
    """
    @brief Refactored pubsub service
    @see http://oceanobservatories.org/spaces/display/CIDev/Pubsub+controller
    @todo Add runtime dependency on exchange management service

    Nomenclature:
    - In AMQP, the hierarchy is xs.xn
    - Topic becomes the routing key
    - XP is a topic exchange
    - XS is hardwired to 'swapmeet' (place to exchange)
    - XP is 'science_data'

    e.g.
    swapmeet / science_data / test.pydap.org:coads.nc

    that last field is subject to argument.
    """
    declare = ServiceProcess.service_declare(name='pubsub',
                                          version='0.1.2',
                                          dependencies=[])

    def slc_init(self):
        self.ems = ExchangeManagementClient(proc=self)
        self.rclient = ResourceClient(proc=self)
        self.mc = MessageClient(proc=self)

        # @todo Lists to replace find/query in registry
        self.xs_list = dict()
        self.xp_list = dict()
        self.topic_list = dict()
        self.pub_list = dict()
        self.sub_list = dict()
        self.q_list = dict()
        self.binding_list = dict()

    @defer.inlineCallbacks
    def op_declare_exchange_space(self, request, headers, msg):
        log.debug('Here we go')
        if request.MessageType != XS_TYPE:
            raise PSSException('Bad message, expected a request type, got %s' % str(request),
                               request.ResponseCodes.BAD_REQUEST)


        log.debug('Calling EMS to create the exchange space...')
        # For now, use timestamp as description
        description = str(time.time())
        xsid = yield self.ems.create_exchangespace(request.exchange_space_name, description)

        log.debug('EMS returns ID %s for name %s' % (xsid.resource_reference.key, request.exchange_space_name))

        # Write ID into registry
        log.debug('Creating resource instance')
        registry_entry = yield self.rclient.create_instance(XS_RES_TYPE, ResourceName=request.exchange_space_name,
                                                            ResourceDescription=request.exchange_space_name)
        # Populate registry entry message
        registry_entry.exchange_space_name = request.exchange_space_name
        registry_entry.exchange_space_id = xsid.resource_reference

        log.debug('Writing resource record')
        xs_resource_id = yield self.rclient.put_instance(registry_entry)

        log.debug('Operation completed, creating response message')
        response = yield self.mc.create_instance(IDLIST_TYPE, MessageName='declare_xs response')
        response.id_list.add()
        response.id_list[0]=xs_resource_id.resource_reference
        response.MessageResponseCode = response.ResponseCodes.OK

        # save to list
        log.debug('Saving to internal list...')
        self.xs_list[xs_resource_id.resource_reference.key] = request.exchange_space_name

        log.debug('Responding...')
        yield self.reply_ok(msg, response)
        log.debug('DXS completed OK')


    @defer.inlineCallbacks
    def op_undeclare_exchange_space(self, request, headers, msg):
        log.debug('UDXP starting')
        # Typecheck the message
        if request.MessageType != REQUEST_TYPE:
            raise PSSException('Bad message type received', request.ResponseCodes.BAD_REQUEST)

        log.debug('Looking for XS entry...')
        try:
            rc = self.xs_list[request.resource_reference]
        except KeyError:
            response = yield self.mc.create_instance(RESPONSE_TYPE)
            response.MessageResponseCode = response.ResponseCodes.NOT_FOUND
            yield self.reply_ok(msg, response)

        # @todo Call EMS to remove the XS
        # @todo Remove resource record too
        log.warn('Here is where we ask EMS to remove the XS')
        response = yield self.mc.create_instance(RESPONSE_TYPE)
        response.MessageResponseCode = response.ResponseCodes.OK

        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_query_exchange_spaces(self, request, headers, msg):
        # Typecheck the message
        if request.MessageType != REGEX_TYPE:
            raise PSSException('Bad message type received', request.ResponseCodes.BAD_REQUEST)

        log.debug('Looking for XS entries...')
        yield self._do_query(request, self.xs_list)

    @defer.inlineCallbacks
    def _do_query(self, request, res_list):
        # This is probably better written as a list comprehension. Or something.
        rc = []
        p = re.compile(request.regex)
        for cur_key, cur_entry in self.res_list.iteritems():
            if p.match(cur_entry):
                rc.append(cur_key)

        log.debug('Matches to "%s" are: "%s"' % (request.regex, str(rc)))
        response = yield self.mc.create_instance(IDLIST_TYPE)
        response.MessageResponseCode = response.ResponseCodes.OK

        idx = 0
        for x in rc:
            response.id_list.add()
            response.id_list[idx] = x
            idx += 1

        yield self.reply_ok(msg, response)

    @defer.inlineCallbacks
    def op_declare_exchange_point(self, request, headers, msg):
        log.debug('Starting DXP')

        if request.MessageType != XP_TYPE:
            raise PSSException('Bad message, expected a request type, got %s' % str(request),
                               request.ResponseCodes.BAD_REQUEST)

        # Lookup the XS ID in the dictionary
        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            log.debug('Found XS %s/%s' % (request.exchange_space_id.key, xs_name))
        except KeyError:
            raise PSSException('Unable to locate XS ID %s' % request.exchange_space_id.key,
                               request.ResponseCodes.BAD_REQUEST)

        # Found XS ID, now call EMS
        description = str(time.time())
        xpid = yield self.ems.create_exchangename(request.exchange_point_name, description, xs_name)

        log.debug('EMS completed, returned XP ID "%s"' % str(xpid))

        xp_resource = yield self.rclient.create_instance(XP_RES_TYPE,
                                                         ResourceName=request.exchange_point_name,
                                                         ResourceDescription=description)

        log.debug('creating xp resource and populating')
        xp_resource.exchange_space_name = xs_name
        xp_resource.exchange_space_id = request.exchange_space_id
        xp_resource.exchange_point_id = xpid
        xp_resource.exchange_point_name = request.exchange_point_name

        log.debug('Saving XP to registry')
        xp_resource_id = yield self.rclient.put_instance(xp_resource)

        log.debug('Creating reply')
        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = xp_resource_id.resource_reference

        log.debug('Saving XPID to internal list')
        self.xp_list[xp_resource_id.resource_reference.key] = request.exchange_point_name

        log.debug('DXP responding')
        yield self.reply_ok(msg, reply)
        log.debug('DXP completed OK')

    @defer.inlineCallbacks
    def op_undeclare_exchange_point(self, request, headers, msg):
        log.debug('UDXP starting')

        if request.MessageType != REQUEST_TYPE:
            raise PSSException('Bad message type',
                               request.ResponseCodes.BAD_REQUEST)


        # Try to lookup the XP in the dictionary
        log.debug('Looking for XP ID %s' % request.resource_reference.key)
        try:
            xp_name = self.xp_list[request.resource_reference.key]
            log.debug('XP found, %s/%s' % (request.resource_reference.key, xp_name))
        except KeyError:
            raise PSSException('Unable to locate XP ID',
                               request.ResponseCodes.BAD_REQUEST)

        # @todo Look up XS via XPID, call EMS to remove same...
        log.warn('This is where the Actual Work Goes...')
        yield self.reply_err(msg)

    @defer.inlineCallbacks
    def op_query_exchange_points(self, request, headers, msg):
        log.debug('Starting XP query')
        # Validate the input
        if request.MessageType != REGEX_TYPE:
            raise PSSException('Bad message type, expected regex query',
                               request.ResponseCodes.BAD_REQUEST)

        if not request.IsFieldSet('regex'):
            raise PSSException('Bad message, regex missing',
                               request.ResponseCodes.BAD_REQUEST)

        # Look 'em up, queue 'em up, head 'em out, raw-hiiiide
        yield self._do_query(request, self.xp_list)

    @defer.inlineCallbacks
    def op_declare_topic(self, request, headers, msg):
        log.debug('Declare topic starting')
        if request.MessageType != TOPIC_TYPE:
            raise PSSException('Bad message type, expected a topic',
                               request.ResponseCodes.BAD_REQUEST)

        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            xp_name = self.xp_list[request.exchange_point_id.key]
        except KeyError:
            log.exception('Error looking up exchange space or point')
            raise PSSException('Unable to look up exchange space or point',
                               request.ResponseCodes.BAD_REQUEST)
        description = str(time.time())
        tid = yield self.ems.create_topic(xs_name, xp_name, request.topic_name, description)

        log.debug('creating and populating the resource')
        topic_resource = yield self.rclient.create_instance(TOPIC_RES_TYPE)
        topic_resource.exchange_space_name = xs_name
        topic_resource.exchange_point_name = xp_name
        topic_resource.topic_name = request.topic_name
        topic_resource.topic_id = tid
        topic_resource.exchange_space_id = request.exchange_space_id
        topic_resource.exchange_point_id = request.exchange_point_id

        log.debug('Saving resource...')
        res_id = yield self.rclient.put_instance(topic_resource)

        log.debug('Creating reply')
        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = res_id.resource_reference

        log.debug('Saving topic to internal list...')
        self.topic_list[res_id.resource_reference.key] = request.topic_name

        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_query_topics(self, request, headers, msg):
        log.debug('topic query starting')
        # Input validation... GIGO, after all. Or should we rename that 'Gigli'?
        if request.MessageType != REGEX_TYPE:
            raise PSSException('Bad message type, expected regex query',
                               request.ResponseCodes.BAD_REQUEST)

        if not request.IsFieldSet('regex'):
            raise PSSException('Bad message, regex missing',
                               request.ResponseCodes.BAD_REQUEST)

        yield self._do_query(request, self.topic_list)

    @defer.inlineCallbacks
    def op_declare_publisher(self, request, headers, msg):
        log.debug('Starting DP')
        if request.MessageType != PUBLISHER_TYPE:
            raise PSSException('Bad message type, expected PublisherMsg',
                               request.ResponseCodes.BAD_REQUEST)

        # Verify that IDs exist - not sure if this is the correct order or not...
        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            xp_name = self.xp_list[request.exchange_point_id.key]
            topic_name = self.topic_list[request.topic_id]
        except KeyError:
            log.exception('Error looking up publisher context!')
            raise PSSException('Bad publisher request, cannot locate context',
                               request.ResponseCodes.BAD_REQUEST)

        log.debug('Publisher context is %s/%s/%s' % (xs_name, xp_name, topic_name))
        description = str(time.time())
        publ_id = yield self.ems.create_publisher(xs_name, xp_name, topic_name,
                                                  request.publisher_name,
                                                  request.credentials, description)
        log.debug('pub id is ' + publ_id.resource_reference.key)

        log.debug('creating and populating the publ resource')
        publ_resource = yield self.rclient.create_instance(PUBLISHER_RES_TYPE)
        publ_resource.exchange_space_id = request.exchange_space_id
        publ_resource.exchange_point_id = request.exchange_point_id
        publ_resource.topic_id = request.topic_id
        publ_resource.publisher_name = request.publisher_name
        publ_resource.credentials = request.credentials
        publ_resource.publisher_id = publ_id

        log.debug('Saving publ resource....')
        publ_res_id = yield self.rclient.put_instance(publ_resource)
        log.debug('Creating reply')
        reply = self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = publ_res_id.resource_reference

        log.debug('Saving publisher to internal list...')
        self.pub_list[publ_res_id.resource_reference.key] = request.publisher_name

        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_undeclare_publisher(self, request, headers, msg):
        log.debug('UDP starting')

        if request.MessageType != REQUEST_TYPE:
            raise PSSException('Bad message type',
                               request.ResponseCodes.BAD_REQUEST)


        # Try to lookup in the dictionary
        log.debug('Looking for publ ID %s' % request.resource_reference.key)
        try:
            pub_name = self.pub_list[request.resource_reference.key]
            log.debug('Publisher found, %s/%s' % (request.resource_reference.key, pub_name))
        except KeyError:
            raise PSSException('Unable to locate publisher ID %s' % request.resource_reference.key,
                               request.ResponseCodes.BAD_REQUEST)

        # @todo Using publisher ID, lookup exchange space and point and topic from registry
        log.warn('This is where the Actual Work Goes...')

        yield self.reply_err(msg)


    @defer.inlineCallbacks
    def op_subscribe(self, request, headers, msg):
        log.debug('PSC subscribe starting')
        if request.MessageType != SUBSCRIBER_TYPE:
            raise PSSException('Bad message type, expected SubscriberMsg',
                               request.ResponseCodes.BAD_REQUEST)

        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            xp_name = self.xp_list[request.exchange_point_id.key]
            topic_name = self.topic_list[request.topic_id.key]
        except KeyError:
            log.exception('Error looking up subscription context!')
            raise PSSException('Bad subscription request, cannot locate context',
                               request.ResponseCodes.BAD_REQUEST)


        # Hmm, is EMS gonna return a string queue name/address or what?
        # Assume a string for now.
        # We return a resource ref, which then must be looked up. Hmm. Change
        # to string return?
        q_name = yield self.ems.subscribe(xs_name, xp_name, topic_name)

        # Save into registry
        log.debug('Saving subscription into registry')
        sub_resource = yield self.rclient.create_instance(SUBSCRIBER_RES_TYPE)
        sub_resource.exchange_space_id = request.exchange_space_id
        sub_resource.exchange_point_id = request.exchange_point_id
        sub_resource.topic_id = request.topic_id
        sub_resource.queue_name = q_name

        sub_res_id = yield self.rclient.put_instance(sub_resource)

        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = sub_res_id.resource_reference

        log.debug('Saving to internal list')
        self.sub_list[sub_res_id.resource_reference.key] = request.subscriber_name
        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_unsubscribe(self, request, headers, msg):
        log.debug('Starting unsub')
        if request.MessageType != REQUEST_TYPE:
            raise PSSException('Bad message type, expected request type',
                               request.ResponseCodes.BAD_REQUEST)

        try:
            sub_name = self.sub_list[request.resource_reference.key]
        except KeyError:
            log.exception('Unable to locate subscription!')
            raise PSSException('Could not locate subscription ID',
                               request.ResponseCodes.BAD_REQUEST)

        # @todo Call EMS, delete from registry
        log.warn('Theres a wee bit of code left to do here....')
        yield self.reply_err(msg)

    @defer.inlineCallbacks
    def op_declare_queue(self, request, headers, msg):
        log.debug('DQ starting')
        if request.MessageType != QUEUE_TYPE:
            raise PSSException('Bad message type, expected QueueMsg',
                               request.ResponseCodes.BAD_REQUEST)

        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            xp_name = self.xp_list[request.exchange_point_id.key]
            topic_name = self.topic_list[request.topic_id]
            sub_name = self.sub_list[request.subscriber_id]
        except KeyError:
            log.exception('Error looking up queue context!')
            raise PSSException('Bad queue request, cannot locate context',
                               request.ResponseCodes.BAD_REQUEST)

        log.debug('Queue context is %s/%s/%s/%s' %
                  (xs_name, xp_name, topic_name, sub_name))

        description = str(time.time())
        q_name = yield self.ems.declare_queue(xs_name, xp_name, topic_name,
                                     request.queue_name, description)

        q_resource = yield self.mc.create_instance(QUEUE_RES_TYPE)
        q_resource.exchange_space_id = request.exchange_space_id
        q_resource.exchange_point_id = request.exchange_point_id
        q_resource.topic_id = request.topic_id
        q_resource.subscriber_id = request.subscriber_id
        q_resource.queue_name = q_name

        q_id = yield self.rclient.put_instance(q_resource)
        log.debug('Saving q into dictionary')
        self.q_list[q_resource.resource_reference.key] = q_name

        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = q_id.resource_reference

        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_add_binding(self, request, headers, msg):
        log.debug('PSC AB starting')
        if request.MessageType != QUEUE_TYPE:
            raise PSSException('Bad message type, expected BindingMsg',
                               request.ResponseCodes.BAD_REQUEST)

        # Hmm, what's required to add a binding?? Something like this?
        rc = yield self.ems.add_binding(request.queue_name, request.binding)

        # If so, do we need registries for this at all?
        yield self.reply_ok(msg)


class PubSubClient(ServiceClient):
    """
    @brief PubSub service client, refactored to use protocol buffer messaging.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = 'pubsub'
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def declare_exchange_space(self, params):
        """
        @brief Declare an exchange space, ok to call more than once (idempotent)
        @param params GPB, 2313/1, with exchange_space_name set to the desired string
        @retval XS ID, GPB 2312/1, if zero-length then an error occurred
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('declare_exchange_space', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def undeclare_exchange_space(self, params):
        """
        @brief Remove an exchange space by ID
        @param params Exchange space ID, GPB 10/1, in field resource_reference
        @retval Generic return GPB 11/1
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('undeclare_exchange_space', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def query_exchange_spaces(self, params):
        """
        @brief List exchange spaces that match a regular expression
        @param params GPB, 2306/1, with 'regex' filled in
        @retval GPB, 2312/1, maybe zero-length if no matches.
        @retval error return also possible
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('query_exchange_spaces', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def declare_exchange_point(self, params):
        """
        @brief Declare/create and exchange point, which is a topic-routed exchange
        @note Must have parent exchange space id before calling this
        @param params GPB 2309/1, with exchange_point_name and exchange_space_id filled in
        @retval GPB 2312/1, zero length if error.
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('declare_exchange_point', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def undeclare_exchange_point(self, params):
        """
        @brief Remove an exchange point by ID
        @param params Exchange point ID, GPB 10/1, in field resource_reference
        @retval Generic return GPB 11/1
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('undeclare_exchange_point', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def query_exchange_points(self, params):
        """
        @brief List exchange points that match a regular expression
        @param params GPB, 2306/1, with 'regex' filled in
        @retval GPB, 2312/1, maybe zero-length if no matches.
        @retval error return also possible
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('query_exchange_points', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def declare_topic(self, params):
        """
        @brief Declare/create a topic in a given xs.xp. A topic is usually a dataset name.
        @param params GPB 2307/1, with xs and xp_ids set
        @retval GPB 2312/1, zero-length if error
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('declare_topic', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def undeclare_topic(self, params):
        """
        @brief Remove a topic by ID
        @param params Topic ID, GPB 10/1, in field resource_reference
        @retval Generic return GPB 11/1
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('undeclare_topic', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def query_topics(self, params):
        """
        @brief List topics that match a regular expression
        @param params GPB, 2306/1, with 'regex' filled in
        @retval GPB, 2312/1, maybe zero-length if no matches.
        @retval error return also possible
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('query_topics', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def declare_publisher(self, params):
        """
        @brief Declare/create a publisher in a given xs.xp.topic.
        @param params GPB 2310/1, with xs, xp and topic_ids set
        @retval GPB 2312/1, zero-length if error
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('declare_publisher', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def undeclare_publisher(self, params):
        """
        @brief Remove a publisher by ID
        @param params Publisher ID, GPB 10/1, in field resource_reference
        @retval Generic return GPB 11/1
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('undeclare_publisher', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def query_publishers(self, params):
        """
        @brief List publishers that match a regular expression
        @param params GPB, 2306/1, with 'regex' filled in
        @retval GPB, 2312/1, maybe zero-length if no matches.
        @retval error return also possible
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('query_publishers', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def undeclare_publisher(self, params):
        """
        @brief Remove a publisher by ID
        @param params Publisher ID, GPB 10/1, in field resource_reference
        @retval Generic return GPB 11/1
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('undeclare_publisher', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def subscribe(self, params):
        """
        @brief The core operation, subscribe to a dataset/source by xs.xp.topic
        @note Not fully fleshed out yet, interface subject to change
        @param params GPB 2311/1
        @retval GPB 2312/1, zero-length if a problem
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('subscribe', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def unsubscribe(self, params):
        """
        @brief Remove a subscription by ID
        @param params Subscription ID, GPB 10/1, in field resource_reference
        @retval Generic return GPB 11/1
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('unsubscribe', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def declare_queue(self, params):
        """
        @brief Create a listener queue for a subscription
        @param GPB 2308/1
        @retval None
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('declare_queue', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def add_binding(self, params):
        """
        @brief Add a binding to an existing queue
        @param params GPB 2314/1
        @retval None
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('add_binding', params)
        defer.returnValue(content)

        
# Spawn off the process using the module name
factory = ProcessFactory(PubSubService)
