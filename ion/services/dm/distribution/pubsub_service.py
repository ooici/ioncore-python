#!/usr/bin/env python

"""
@file ion/services/dm/distribution/pubsub_service.py
@package ion.services.dm.distribution.pubsub
@author Paul Hubbard
@author Michael Meisinger
@author David Stuebe
@brief service for publishing on data streams, and for subscribing to streams.
The service includes methods for defining topics, defining publishers, publishing,
and defining subscriptions. See the PubSubClient for API documentation.
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
IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

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

    that last field is subject to argument/debate/refactoring.

    @note PSC uses 'Niemand' here and there as a placeholder name
    @note PSC uses the current timestamp, string format, as a placeholder description
    """
    declare = ServiceProcess.service_declare(name='pubsub',
                                          version='0.1.2',
                                          dependencies=[])

    def slc_init(self):
        self.ems = ExchangeManagementClient(proc=self)
        self.rclient = ResourceClient(proc=self)
        self.mc = MessageClient(proc=self)

        # @bug Dictionaries to cover for lack of find/query in registry
        self.xs_list = dict()
        self.xp_list = dict()
        self.topic_list = dict()
        self.pub_list = dict()
        self.sub_list = dict()
        self.q_list = dict()
        self.binding_list = dict()

    def _check_msg_type(self, request, expected_type):
        """
        @brief Simple helper routine to validate the GPB that arrives against what's expected.
        Raising the exception will filter all the way back to the service client.
        @param request Incoming message, GPB assumed
        @param expected_type Typedef from object utils
        """
        if request.MessageType != expected_type:
            log.error('Bad message type, throwing exception')
            raise PSSException('Bad message type!',
                               request.ResponseCodes.BAD_REQUEST)

    def _key_to_idref(self, key_string, object):
        """
        From a CASref key, create a full-on casref.
        @param key_string String, from casref key
        @param object Reply object we are modifying (in-place)
        @retval None
        """
        my_ref = object.CreateObject(IDREF_TYPE)
        my_ref.key = key_string
        idx = len(object.id_list)
        object.id_list.add()
        log.debug('Adding to index %d' % idx)
        object.id_list[idx] = my_ref

    def _obj_to_ref(self, object):
        """
        @brief Generate a casref/idref from an object, so that proto bufs requiring
        CASrefs will work. It's a one-liner, but worth calling out.
        @param object Yep, object to reference
        @retval Reference to object
        """
        return self.rclient.reference_instance(object)

    @defer.inlineCallbacks
    def _do_query(self, request, res_list, msg):
        """
        @brief Query internal dictionaries, create reply message, send same. Helper for the
        various queries.
        """
        # This is probably better written as a list comprehension. Or something.
        idlist = []
        p = re.compile(request.regex)
        for cur_key, cur_entry in res_list.iteritems():
            if p.match(cur_entry):
                idlist.append(cur_key)

        log.debug('Matches to "%s" are: "%s"' % (request.regex, str(idlist)))
        response = yield self.mc.create_instance(IDLIST_TYPE)
        response.MessageResponseCode = response.ResponseCodes.OK

        # For each string in the dictionary, inflate into a casref/idref and add to message
        for key in idlist:
            self._key_to_idref(key, response)

        log.debug('Query complete')
        yield self.reply_ok(msg, response)

    def _reverse_find(self, data, search_value):
        """
        @brief Look for a given value in a provided dictionary, return key that corresponds.
        @note Probably a better way to solve this.
        @note To emulate the python list, it raises KeyError if not found.
        """
        rc = None
        if not search_value in data.values():
            raise KeyError('%s not in data', search_value)

        for key, value in data.iteritems():
            if value == search_value:
                rc = key

        return rc

    @defer.inlineCallbacks
    def op_declare_exchange_space(self, request, headers, msg):
        """
        @see PubSubClient.declare_exchange_space
        """
        log.debug('DXS starting')
        self._check_msg_type(request, XS_TYPE)

        # Already declared?
        try:
            key = self._reverse_find(self.xs_list, request.exchange_space_name)
            log.info('Exchange space "%s" already created, returning' % request.exchange_space_name)
            response = yield self.mc.create_instance(IDLIST_TYPE)
            self._key_to_idref(key, response)
            yield self.reply_ok(msg, response)
            return
        except KeyError:
            log.debug('XS not found, will go ahead and create')

        log.debug('Calling EMS to create the exchange space "%s"...'
                % request.exchange_space_name)
        # For now, use timestamp as description
        description = str(time.time())
        xsid = yield self.ems.create_exchangespace(request.exchange_space_name, description)

        log.debug('EMS returns ID %s for name %s' %
                  (xsid.resource_reference.key, request.exchange_space_name))

        # Write ID into registry
        log.debug('Creating resource instance')
        registry_entry = yield self.rclient.create_instance(XS_RES_TYPE, 'Niemand')

        # Populate registry entry message
        log.debug('Populating resource instance')
        registry_entry.exchange_space_name = request.exchange_space_name
        registry_entry.exchange_space_id = xsid.resource_reference

        log.debug('Writing resource record')
        yield self.rclient.put_instance(registry_entry)
        log.debug('Getting resource ID')
        xs_resource_id = self._obj_to_ref(registry_entry)

        log.debug('Operation completed, creating response message')

        response = yield self.mc.create_instance(IDLIST_TYPE)
        log.debug('Populating return message')
        response.id_list.add()
        response.id_list[0] = xs_resource_id
        response.MessageResponseCode = response.ResponseCodes.OK

        # save to list
        log.debug('Saving to internal list...')
        self.xs_list[xs_resource_id.key] = request.exchange_space_name

        log.debug('Responding...')
        yield self.reply_ok(msg, response)
        log.debug('DXS completed OK')

    @defer.inlineCallbacks
    def op_undeclare_exchange_space(self, request, headers, msg):
        """
        @see PubSubClient.undeclare_exchange_space
        """
        log.debug('UDXS starting')
        self._check_msg_type(request, REQUEST_TYPE)

        log.debug('Looking for XS entry...')
        try:
            rc = self.xs_list[request.resource_reference.key]
        except KeyError:
            response = yield self.mc.create_instance(RESPONSE_TYPE)
            response.MessageResponseCode = response.ResponseCodes.NOT_FOUND
            yield self.reply_ok(msg, response)

        log.debug('Found XS %s' % rc)
        # @todo Call EMS to remove the XS
        # @todo Remove resource record too
        log.warn('Here is where we ask EMS to remove the XS')
        del(self.xs_list[request.resource_reference.key])

        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_query_exchange_spaces(self, request, headers, msg):
        """
        @see PubSubClient.query_exchange_spaces
        """
        log.debug('qxs starting')
        self._check_msg_type(request, REGEX_TYPE)

        log.debug('Looking for XS entries...')
        yield self._do_query(request, self.xs_list, msg)

    @defer.inlineCallbacks
    def op_declare_exchange_point(self, request, headers, msg):
        """
        @see PubSubClient.declare_exchange_point
        """

        log.debug('Starting DXP')
        self._check_msg_type(request, XP_TYPE)

        # Already declared?
        try:
            key = self._reverse_find(self.xp_list, request.exchange_point_name)
            log.info('Exchange point "%s" already created, returning' %
                     request.exchange_point_name)
            response = yield self.mc.create_instance(IDLIST_TYPE)
            self._key_to_idref(key, response)
            yield self.reply_ok(msg, response)
            return
        except KeyError:
            log.debug('XP not found, will go ahead and create')

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

        log.debug('EMS completed, returned XP ID "%s"' % xpid.resource_reference.key)

        xp_resource = yield self.rclient.create_instance(XP_RES_TYPE, 'Niemand')

        log.debug('creating xp resource and populating')

        xp_resource.exchange_space_name = xs_name
        xp_resource.exchange_space_id = request.exchange_space_id
        xp_resource.exchange_point_id = xpid.resource_reference
        xp_resource.exchange_point_name = request.exchange_point_name

        log.debug('Saving XP to registry')
        yield self.rclient.put_instance(xp_resource)
        xp_resource_id = self._obj_to_ref(xp_resource)

        log.debug('Creating reply')
        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = xp_resource_id

        log.debug('Saving XPID to internal list')
        self.xp_list[xp_resource_id.key] = request.exchange_point_name

        log.debug('DXP responding')
        yield self.reply_ok(msg, reply)
        log.debug('DXP completed OK')

    @defer.inlineCallbacks
    def op_undeclare_exchange_point(self, request, headers, msg):
        """
        @see PubSubClient.declare_exchange_point
        """
        log.debug('UDXP starting')
        self._check_msg_type(request, REQUEST_TYPE)

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
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_query_exchange_points(self, request, headers, msg):
        """
        @see PubSubClient.query_exchange_points
        """

        log.debug('Starting XP query')
        self._check_msg_type(request, REGEX_TYPE)

        if not request.IsFieldSet('regex'):
            raise PSSException('Bad message, regex missing',
                               request.ResponseCodes.BAD_REQUEST)

        # Look 'em up, queue 'em up, head 'em out, raw-hiiiide
        yield self._do_query(request, self.xp_list, msg)

    @defer.inlineCallbacks
    def op_declare_topic(self, request, headers, msg):
        """
        @see PubSubClient.declare_exchange_topic
        """
        log.debug('Declare topic starting')
        self._check_msg_type(request, TOPIC_TYPE)

        # Already declared?
        try:
            key = self._reverse_find(self.topic_list, request.topic_name)
            log.info('Topic "%s" already created, returning' % request.topic_name)
            response = yield self.mc.create_instance(IDLIST_TYPE)
            self._key_to_idref(key, response)
            yield self.reply_ok(msg, response)
            return
        except KeyError:
            log.debug('Topic not found, will go ahead and create')

        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            xp_name = self.xp_list[request.exchange_point_id.key]
        except KeyError:
            log.exception('Error looking up exchange space or point')
            raise PSSException('Unable to look up exchange space or point',
                               request.ResponseCodes.BAD_REQUEST)

        log.debug('Creating and populating the resource')
        topic_resource = yield self.rclient.create_instance(TOPIC_RES_TYPE, 'Niemand')
        topic_resource.exchange_space_name = xs_name
        topic_resource.exchange_point_name = xp_name
        topic_resource.topic_name = request.topic_name
        topic_resource.exchange_space_id = request.exchange_space_id
        topic_resource.exchange_point_id = request.exchange_point_id

        log.debug('Saving resource...')
        yield self.rclient.put_instance(topic_resource)

        log.debug('Creating reply')
        reply = yield self.mc.create_instance(IDLIST_TYPE)

        log.debug('Creating reference to resource for return value')
        # We return by reference, so create same
        topic_ref = self._obj_to_ref(topic_resource)

        reply.id_list.add()
        reply.id_list[0] = topic_ref

        log.debug('Saving topic "%s"/"%s" to internal list...' % (topic_ref.key, request.topic_name))
        self.topic_list[topic_ref.key] = request.topic_name

        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_undeclare_topic(self, request, headers, msg):
        """
        @see PubSubClient.undeclare_topic
        """

        log.debug('UDT starting')
        self._check_msg_type(request, REQUEST_TYPE)

        try:
            log.debug('Looking for topic %s...' % request.resource_reference.key)
            topic_name = self.topic_list[request.resource_reference.key]
        except KeyError:
            log.exception('Unable to locate topic!')
            response = yield self.mc.create_instance(RESPONSE_TYPE)
            response.MessageResponseCode = response.ResponseCodes.NOT_FOUND
            yield self.reply_ok(msg, response)

        log.debug('Topic %s found' % topic_name)
        # @todo Remove instance from resource registry
        del(self.topic_list[request.resource_reference.key])
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_query_topics(self, request, headers, msg):
        """
        @see PubSubClient.query_topics
        """

        log.debug('topic query starting')
        # Input validation... GIGO, after all. Or should we rename that 'Gigli'?
        self._check_msg_type(request, REGEX_TYPE)
        if not request.IsFieldSet('regex'):
            raise PSSException('Bad message, regex missing',
                               request.ResponseCodes.BAD_REQUEST)

        yield self._do_query(request, self.topic_list, msg)

    @defer.inlineCallbacks
    def op_declare_publisher(self, request, headers, msg):
        """
        @see PubSubClient.declare_exchange_publisher
        """

        log.debug('Starting DP')
        self._check_msg_type(request, PUBLISHER_TYPE)

        # Verify that IDs exist - not sure if this is the correct order or not...
        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            xp_name = self.xp_list[request.exchange_point_id.key]
            topic_name = self.topic_list[request.topic_id.key]
        except KeyError:
            log.exception('Error looking up publisher context!')
            raise PSSException('Bad publisher request, cannot locate context',
                               request.ResponseCodes.BAD_REQUEST)

        log.debug('Publisher context is %s/%s/%s' % (xs_name, xp_name, topic_name))

        log.debug('Creating and populating the publisher resource...')
        publ_resource = yield self.rclient.create_instance(PUBLISHER_RES_TYPE, 'Niemand')
        publ_resource.exchange_space_id = request.exchange_space_id
        publ_resource.exchange_point_id = request.exchange_point_id
        publ_resource.topic_id = request.topic_id
        publ_resource.publisher_name = request.publisher_name
        publ_resource.credentials = request.credentials

        log.debug('Saving publisher resource....')
        yield self.rclient.put_instance(publ_resource)

        # Need a reference return value
        pub_ref = self._obj_to_ref(publ_resource)

        log.debug('Creating reply')
        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = pub_ref

        log.debug('Saving publisher to internal list...')
        self.pub_list[pub_ref.key] = request.publisher_name

        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_undeclare_publisher(self, request, headers, msg):
        """
        @see PubSubClient.undeclare_publisher
        """
        log.debug('UDP starting')
        self._check_msg_type(request, REQUEST_TYPE)

        # Try to lookup in the dictionary
        log.debug('Looking for publ ID %s' % request.resource_reference.key)
        try:
            pub_name = self.pub_list[request.resource_reference.key]
            log.debug('Publisher found, %s/%s' % (request.resource_reference.key, pub_name))
        except KeyError:
            raise PSSException('Unable to locate publisher ID %s' % request.resource_reference.key,
                               request.ResponseCodes.BAD_REQUEST)

        del(self.pub_list[request.resource_reference.key])
        # @todo Delete from registry
        log.warn('This is where the Actual Work Goes...')

        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_query_publishers(self, request, headers, msg):
        log.debug('QP starting')
        self._check_msg_type(request, REGEX_TYPE)

        if not request.IsFieldSet('regex'):
            raise PSSException('Bad message, regex missing',
                               request.ResponseCodes.BAD_REQUEST)

        # Look 'em up, queue 'em up, head 'em out, raw-hiiiide
        yield self._do_query(request, self.pub_list, msg)

    @defer.inlineCallbacks
    def op_subscribe(self, request, headers, msg):
        """
        @see PubSubClient.subscribe
        """

        log.debug('PSC subscribe starting')
        self._check_msg_type(request, SUBSCRIBER_TYPE)

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
        # @todo Declare queue and binding??!

        # Save into registry
        log.debug('Saving subscription into registry')
        sub_resource = yield self.rclient.create_instance(SUBSCRIBER_RES_TYPE, 'Niemand')
        sub_resource.exchange_space_id = request.exchange_space_id
        sub_resource.exchange_point_id = request.exchange_point_id
        sub_resource.topic_id = request.topic_id
        sub_resource.queue_name = str(time.time()) # Hack!

        yield self.rclient.put_instance(sub_resource)

        sub_ref = self._obj_to_ref(sub_resource)

        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = sub_ref

        log.debug('Saving %s/%s to internal list' % (sub_ref.key, request.subscriber_name))
        self.sub_list[sub_ref.key] = request.subscriber_name
        yield self.reply_ok(msg, reply)

    @defer.inlineCallbacks
    def op_unsubscribe(self, request, headers, msg):
        """
        @see PubSubClient.unsubscribe
        """

        log.debug('Starting unsub')
        self._check_msg_type(request, REQUEST_TYPE)

        try:
            sub_name = self.sub_list[request.resource_reference.key]
        except KeyError:
            log.exception('Unable to locate subscription!')
            raise PSSException('Could not locate subscription ID',
                               request.ResponseCodes.BAD_REQUEST)

        log.debug('Deleting subscription for %s' % sub_name)
        del(self.sub_list[request.resource_reference.key])

        # @todo Call EMS, delete from registry
        log.warn('Theres a wee bit of code left to do here....')
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_query_subscribers(self, request, headers, msg):
        log.debug('QS starting')
        self._check_msg_type(request, REGEX_TYPE)

        if not request.IsFieldSet('regex'):
            raise PSSException('Bad message, regex missing',
                               request.ResponseCodes.BAD_REQUEST)

        # Look 'em up, queue 'em up, head 'em out, raw-hiiiide
        yield self._do_query(request, self.sub_list, msg)

    @defer.inlineCallbacks
    def op_declare_queue(self, request, headers, msg):
        """
        @see PubSubClient.declare_queue
        """
        log.debug('DQ starting')
        self._check_msg_type(request, QUEUE_TYPE)

        try:
            xs_name = self.xs_list[request.exchange_space_id.key]
            xp_name = self.xp_list[request.exchange_point_id.key]
            topic_name = self.topic_list[request.topic_id.key]
        except KeyError:
            log.exception('Error looking up queue context!')
            raise PSSException('Bad queue request, cannot locate context',
                               request.ResponseCodes.BAD_REQUEST)

        log.debug('Queue context is %s/%s/%s' %
                  (xs_name, xp_name, topic_name))

        description = str(time.time())

        log.debug('Calling EMS to make the q')
        yield self.ems.create_queue(request.queue_name, description, xs_name, xp_name)
        
        log.debug('Creating registry object')
        q_resource = yield self.rclient.create_instance(QUEUE_RES_TYPE, 'Niemand')

        q_resource.exchange_space_id = request.exchange_space_id
        q_resource.exchange_point_id = request.exchange_point_id
        q_resource.topic_id = request.topic_id
        q_resource.queue_name = request.queue_name

        log.debug('Saving q into registry')
        yield self.rclient.put_instance(q_resource)
        log.debug('Creating reference')
        q_ref = self._obj_to_ref(q_resource)

        log.debug('Saving q into dictionary')
        self.q_list[q_ref.key] = request.queue_name

        reply = yield self.mc.create_instance(IDLIST_TYPE)
        reply.id_list.add()
        reply.id_list[0] = q_ref

        yield self.reply_ok(msg, reply)


    @defer.inlineCallbacks
    def op_undeclare_queue(self, request, headers, msg):
        """
        @see PubSubClient.undeclare_queue
        @note Possible error if queue declared more than once and then deleted once
        """
        log.debug('Undeclare_q starting')
        self._check_msg_type(request, REQUEST_TYPE)

        # Try to lookup in the dictionary
        log.debug('Looking for queue ID %s' % request.resource_reference.key)
        try:
            q_name = self.q_list[request.resource_reference.key]
            log.debug('Q found, %s/%s' % (request.resource_reference.key, q_name))
        except KeyError:
            raise PSSException('Unable to locate queue ID %s' % request.resource_reference.key,
                               request.ResponseCodes.BAD_REQUEST)

        del(self.q_list[request.resource_reference.key])
        # @todo Delete from registry
        log.warn('This is where the Actual Work Goes...')

        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_add_binding(self, request, headers, msg):
        """
        @see PubSubClient.add_binding
        """
        log.debug('PSC AB starting')
        self._check_msg_type(request, BINDING_TYPE)

        log.debug('Looking up queue for binding....')
        try:
            q_id = self._reverse_find(self.q_list, request.queue_name)
            q_entry = yield self.rclient.get_instance(q_id)

            xs_name = self.xs_list[q_entry.exchange_space_id.key]
            xp_name = self.xp_list[q_entry.exchange_point_id.key]
            topic_name = self.topic_list[q_entry.topic_id.key]
        except KeyError:
            log.exception('Unable to locate queue for binding!')
            raise PSSException('AB error in lookup',
                               request.ResponseCodes.BAD_REQUEST)

        log.debug('Ready for EMS with %s/%s/%s and %s' % \
                  (xs_name, xp_name, topic_name, request.queue_name))
        
        description = str(time.time())
        rc = yield self.ems.create_binding('NoName', description,
                                           xs_name, xp_name, request.queue_name, topic_name)

        b_resource = yield self.rclient.create_instance(BINDING_RES_TYPE, 'Niemand')
        b_resource.queue_name = request.queue_name
        b_resource.binding = request.binding
        b_resource.queue_id = self._obj_to_ref(q_entry)

        yield self.rclient.put_instance(b_resource)

        log.debug('Binding added')
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
        @GPB{Input,2313,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,10,1}
        @GPB{Returns,11,1}
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
        @GPB{Input,2306,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,2309,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,10,1}
        @GPB{Returns,11,1}
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
        @GPB{Input,2306,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,2307,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,10,1}
        @GPB{Returns,11,1}
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
        @GPB{Input,2306,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,2310,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,10,1}
        @GPB{Returns,11,1}
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
        @GPB{Input,2306,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,10,1}
        @GPB{Returns,11,1}
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
        @GPB{Input,2311,1}
        @GPB{Returns,2312,1}
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
        @GPB{Input,10,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('unsubscribe', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def query_subscribers(self, params):
        """
        @brief List subscriber that match a regular expression
        @param params @GPB(2306, 1) with 'regex' filled in
        @retval GPB, 2312/1, maybe zero-length if no matches.
        @retval error return also possible
        @GPB{Input,2306,1}
        @GPB{Returns,2312,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('query_subscribers', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def declare_queue(self, params):
        """
        @brief Create a listener queue for a subscription
        @param GPB 2308/1
        @retval None
        @GPB{Input,2308,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('declare_queue', params)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def undeclare_queue(self, params):
        """
        @brief Undeclare (remove) a queue
        @param GPB 10/1, queue ID
        @retval OK or error
        @GPB{Input,10,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('undeclare_queue', params)
        defer.returnValue(content)


    @defer.inlineCallbacks
    def add_binding(self, params):
        """
        @brief Add a binding to an existing queue
        @param params GPB 2314/1
        @GPB{Input,2314,1}
        @retval None
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('add_binding', params)
        defer.returnValue(content)

        
# Spawn off the process using the module name
factory = ProcessFactory(PubSubService)
