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

class PSSException(Exception):
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
        log.debug('PubSubService starting')
        self.ems = ExchangeManagementClient(proc=self)
        log.debug('Creating ResourceClient')
        self.rclient = ResourceClient(proc=self)
        self.mc = MessageClient(proc=self)

        log.debug('PSS slc_init completed')

    # Protocol entry points. Responsible for parsing and unpacking arguments
    @defer.inlineCallbacks
    def op_declare_exchange_space(self, request, headers, msg):
        log.debug('Here we go')
        if request.MessageType != REQUEST_TYPE:
            raise PSSException('Bad message, expected a request type, got %s' % str(request))


        log.debug('Calling EMS to create the exchange space...')
        # For now, use timestamp as description
        description = str(time.time())
        id = yield self.ems.create_exchangespace(request.exchange_space_name, description=description)

        log.debug('EMS returns ID %s' % id)

        # Write ID into registry
        log.debug('Creating RC instance')
        xs = yield self.rclient.create_instance(XS_TYPE, ResourceName=request.exchange_space_name, ResourceDescription=description)
        log.debug('Writing RC record')
        yield self.rclient.put_instance(xs)

        log.debug('Operation completed, creating response message')

        response = yield self.mc.create_instance(RESPONSE_TYPE, MessageName='declare_xs response')

        log.debug('Response created')

        response.resource_reference = self.rclient.reference_instance(id)

        response.result = 'OK'

        log.debug('Responding...')

        yield self.reply_ok(msg, response)
        log.debug('DXS completed')


    @defer.inlineCallbacks
    def op_declare_topic_tree(self, content, headers, msg):
        try:
            xs_name = content['exchange_space_name']
            tt_name = content['topic_tree_name']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, {'value': estr})
            defer.returnValue(None)

        rc = yield self.declare_topic_tree(xs_name, tt_name)
        yield self.reply_ok(msg, {'value': rc})

    def op_undeclare_topic_tree(self, content, headers, msg):
        try:
            tt_id = content['topic_tree_id']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            self.reply_err(msg, {'value': estr})
            return

        rc = self.undeclare_topic_tree(tt_id)
        self.reply_ok(msg, {'value': rc})

    def op_query_topic_trees(self, content, headers, msg):
        try:
            t_regex = content['topic_regex']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            self.reply_err(msg, {'value': estr})
            return
 
        rc = self.query_topic_trees(t_regex)
        self.reply_ok(msg, {'value': rc})
 
    @defer.inlineCallbacks
    def op_define_topic(self, content, headers, msg):
        try:
            tt_id = content['topic_tree_id']
            t_name = content['topic_name']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, {'value': estr})
            defer.returnValue(None)

        rc = yield self.define_topic(tt_id, t_name)
        yield self.reply_ok(msg, {'value': rc})

    def op_query_topics(self, content, headers, msg):
        try:
            xp_name = content['exchange_point_name']
            t_regex = content['topic_regex']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            self.reply_err(msg, {'value': estr})
            return

        rc = self.query_topics(xp_name, t_regex)
        self.reply_ok(msg, {'value': rc})

    def op_define_publisher(self, content, headers, msg):
        try:
            tt_id = content['topic_tree_id']
            topic_id = content['topic_id']
            p_name = content['publisher_name']
            cred = content['credentials']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            self.reply_err(msg, {'value': estr})
            return

        rc = self.define_publisher(tt_id, topic_id, p_name, cred)
        self.reply_ok(msg, {'value': rc})

    def op_subscribe(self, content, headers, msg):
        try:
            t_regex = content['topic_regex']
            xs_name = content['exchange_space_name']
            tt_name = content['topic_tree_name']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            self.reply_err(msg, {'value': estr})
            return

        rc = self.subscribe(xs_name, tt_name, t_regex)
        self.reply_ok(msg, {'value': rc})

    def op_unsubscribe(self, content, headers, msg):
        try:
            s_id = content['subscription_id']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            self.reply_err(msg, {'value': estr})
            return

        rc = self.unsubscribe(s_id)
        self.reply_ok(msg, {'value': rc})

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

    def undeclare_topic_tree(self, topic_tree_id):
        """
        @brief Remove a topic tree
        @param topic_tree_id ID, as returned from declare_topic_tree
        @retval None
        """
        log.error('UDTT not implemented')

    def query_topic_trees(self, topic_regex):
        """
        @brief Registry query, return all trees that match the regex
        @param topic_regex Regular expression to match against
        @retval List, possibly empty, of topic tree names
        """
        log.error('QTT not implemented')

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


    def query_topics(self, exchange_point_name, topic_regex):
        """
        @brief Query topics within an exchange point
        @param exchange_point_name Exchange point to inspect (scope)
        @param topic_regex Regex to match
        @retval List, possibly empty, of topic names
        """
        log.error('QT not implemented')

    def define_publisher(self, topic_tree_id, topic_id, publisher_name, credentials=None):
        """
        @brief Called by the publisher, this drops through to the resource registry
        @param topic_tree_id Tree where we'll publish
        @param publisher_name Human-readable publisher ID string, e.g. "Doc X's buoy data for NYC harbor"
        @param credentials Unused hook for auth*
        @retval Transciever instance with send() method hooked up to correct topic
        """
        log.error('DP not implemented')

    def subscribe(self, xs_name, tt_name, topic_regex):
        """
        @brief Called by subscribers, this calls the EMS to setup the data flow
        @param xs_name Exchange space name
        @param tt_name Topic tree name
        @param topic_regex Topic of interest. If no publishers, then no data, but no error
        @note Order of calls on publish/subscribe does not matter
        @note creates the queue via EMS
        @retval Address of queue for ondata() callback and resource id
        """
        log.error('Sub not implemented')

    def unsubscribe(self, subscription_id):
        """
        @brief Remove subscription
        @param subscription_id ID from subscribe calS
        @retval OK if no problems, error otherwise
        """
        log.error('Unsub not implemented')

class OldStylePubSubClient(ServiceClient):
    """
    @brief Refactor of client for new interfaces
    @see http://oceanobservatories.org/spaces/display/CIDev/Pubsub+controller
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "pubsub"
        ServiceClient.__init__(self, proc, **kwargs)

    def declare_exchange_space(self, exchange_space_name):
        pass

    def undeclare_exchange_space(self, exchange_space_id):
        pass

    def query_exchange_spaces(self, exchange_space_regex):
        pass

    def declare_exchange_point(self, exchange_space_id, exchange_point_name):
        pass

    def undeclare_exchange_point(self, exchange_point_id):
        pass

    def query_exchange_points(self, exchange_point_regex):
        pass

    def declare_topic(self, exchange_space_id, exchange_point_id, topic_name):
        pass

    def undeclare_topic(self, topic_id):
        pass

    def query_topics(self, topic_regex):
        pass

    def declare_publisher(self, exchange_space_id, exchange_point_id, topic_id, publisher_name, credentials=None):
        pass

    def undeclare_publisher(self, publisher_id):
        pass

    def query_publishers(self, publisher_regex):
        pass

    def subscribe(self, exchange_space_id, exchange_point_id, topic_regex, use_queue=None):
        pass

    def unsubscribe(self, subscription_id):
        pass

    def create_queue(self, exchange_space_id, exchange_point_id):
        pass


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

    def declare_exchange_point(self, params):
        pass

    def undeclare_exchange_point(self, params):
        pass

    def query_exchange_points(self, params):
        pass

    def declare_topic(self, params):
        pass

    def undeclare_topic(self, params):
        pass

    def query_topics(self, params):
        pass

    def declare_publisher(self, params):
        pass

    def undeclare_publisher(self, params):
        pass

    def query_publishers(self, params):
        pass

    def undeclare_publisher(self, params):
        pass

    def subscribe(self, params):
        pass

    def unsubscribe(self, params):
        pass

    def create_queue(self, params):
        pass

    def add_binding(self, params):
        pass
    
# Spawn off the process using the module name
factory = ProcessFactory(PubSubService)
