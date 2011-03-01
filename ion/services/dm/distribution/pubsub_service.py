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

from twisted.internet import defer

from ion.core.exception import ApplicationError
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core import ioninit
import ion.util.ionlog
from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceClientError
from ion.services.coi.exchange.exchange_management import ExchangeManagementClient

# Global objects
CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)

# References to protobuf message/object definitions
TOPIC_TYPE = object_utils.create_type_identifier(object_id=2307, version=1)

# Generic request and response wrapper message types
REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)

# Query and response types
REGEX_TYPE = object_utils.create_type_identifier(object_id=2306, version=1)
IDLIST_TYPE = object_utils.create_type_identifier(object_id=2312, version=1)
XS_TYPE = object_utils.create_type_identifier(object_id=2313, version=1)
XP_TYPE = object_utils.create_type_identifier(object_id=2309, version=1)

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

    # Protocol entry points. Responsible for parsing and unpacking arguments
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

        log.debug('EMS returns ID %s' % xsid.resource_reference)

        # Write ID into registry
        log.debug('Creating RC instance')
        xs = yield self.rclient.create_instance(XS_TYPE, ResourceName=request.exchange_space_name,
                                                ResourceDescription=description)
        log.debug('Writing RC record')
        yield self.rclient.put_instance(xs)

        log.debug('Operation completed, creating response message')

        response = yield self.mc.create_instance(IDLIST_TYPE, MessageName='declare_xs response')

        log.debug('Response message created')

        response.id_list.add()
        response.id_list[0]=xsid.resource_reference

        response.MessageResponseCode = response.ResponseCodes.OK

        # save to list
        log.debug('Saving to internal list...')
        self.xs_list[xsid.resource_reference] = request.exchange_space_name

        log.debug('Responding...')

        yield self.reply_ok(msg, response)
        log.debug('DXS completed')


    @defer.inlineCallbacks
    def op_undeclare_exchange_space(self, request, headers, msg):

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

        log.warn('Here is where we ask EMS to remove the XS')
        response = yield self.mc.create_instance(RESPONSE_TYPE)
        response.MessageResponseCode = response.ResponseCodes.OK

        yield self.reply_ok(msg, response)



    ##############################################################    
    # API-style entry points. Akin to the twisted protocol/factory
    @defer.inlineCallbacks
    def declare_topic_tree(self, exchange_space_name, topic_tree_name):
        """
        @brief Create a topic tree
        @param exchange_space_name Exchange space where the tree will live
        @param topic_tree_name Name of the tree to create
        @retval Topic tree ID on success, None if failure
        """
        log.debug('Creating exchange space "%s"' % exchange_space_name)
        yield self.ems.create_exchangespace(exchange_space_name, 'Default exchange space')

        log.debug('Calling EMS to create topic tree "%s/%s"' % (exchange_space_name, topic_tree_name))
        rc = yield self.ems.create_exchangename(topic_tree_name, 'New topic tree', exchange_space_name)
        if rc == None:
            log.error('Error in creating exchange name (topic tree')
            defer.returnValue(None)
        log.debug('EMS returned "%s"' % str(rc))

        log.debug('Writing topic tree into registry')
        # Now need to write new topic tree into registry
        ttree = yield self.rclient.create_instance(TT_TYPE, name=topic_tree_name, description='New topic tree')

        ttree.exchange_space_name = exchange_space_name
        ttree.topic_tree_name = topic_tree_name

        log.debug('About to push topic tree into registry')
        yield self.rclient.put_instance(ttree, 'declare_topic_tree')
        log.debug('Wrote TT, id is %s' % ttree.ResourceIdentity)
        log.debug("Attach a topic with:  client.define_topic('%s', 'topic_name')" % (ttree.ResourceIdentity))
        
        defer.returnValue(ttree.ResourceIdentity)


    @defer.inlineCallbacks
    def define_topic(self, topic_tree_id, topic_name):
        """
        @brief Within a topic tree, define a topic. Usually a dataset name by convention.
        @param topic_tree_id ID, as returned from op_declare_topic_tree
        @param topic_name Name to declare
        @retval Topic ID, or None if error
        """
        log.debug('Creating and populating dataset message/object')

        # Step 1: Get a handle to the topic's prospective tree
        try:
            ttree = yield self.rclient.get_instance(topic_tree_id)
        except ResourceClientError, ex:
            log.warn('Could not retreive topic tree instance for given topic_tree_id:  %s' % (ex))
            
        # Step 2: Create the topic resource
        cstr = "%s/%s" % (topic_tree_id, topic_name)

        topic = yield self.rclient.create_instance(TOPIC_TYPE, ResourceName=topic_name, ResourceDescription=cstr)
        topic.topic_name = topic_name
        topic.routing_key = cstr
        
        # Step 3: @todo: Create an association between the given topic_tree and the topic being created here
        
        # Step 4: Store changes to the topic
        # @todo: Store changes to whichever resource represents the association between tree and topic
        log.debug('Topic object created, pushing/committing "%s"' % cstr)

        yield self.rclient.put_instance(topic, 'Adding dataset/topic %s' % cstr)
        log.debug('Commit completed, %s' % topic.ResourceIdentity)
        defer.returnValue(topic.ResourceIdentity)


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
    def create_queue(self, params):
        """
        @brief Create a listener queue for a subscription
        @param GPB 2308/1
        @retval None
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('create_queue', params)
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
