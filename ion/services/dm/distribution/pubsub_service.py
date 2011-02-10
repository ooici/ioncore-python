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

import ion.util.ionlog

import time

from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
#from ion.services.dm.distribution import pubsub_registry
from ion.core import ioninit
from ion.core.object import object_utils
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
from ion.services.coi.exchange.exchange_management import ExchangeManagementClient

# Global objects
CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)

# References to protobuf message/object definitions
DSET_TYPE = object_utils.create_type_identifier(object_id=2301, version=1)
TT_TYPE = object_utils.create_type_identifier(object_id=2306, version=1)

class PubSubService(ServiceProcess):
    """
    @brief Refactored pubsub service
    @see http://oceanobservatories.org/spaces/display/CIDev/Pubsub+controller
    @todo Add runtime dependency on exchange management service

    Hierarchy is
    Exchange space => Topic tree => topic name

    Where 'topic tree' is another name for exchange point.

    e.g.
    OOICI / DM / science data / test.pydap.org:coads.nc
    """
    declare = ServiceProcess.service_declare(name='pubsub',
                                          version='0.1.1',
                                          dependencies=[])

    def slc_init(self):
        log.debug('PSC starting, creating EMS client')
        self.ems = ExchangeManagementClient(proc=self)
        log.debug('Creating ResourceClient')
        self.rclient = ResourceClient(proc=self)

        log.debug('PSC slc_init completed')
    # Protocol entry points. Responsible for parsing and unpacking arguments
    @defer.inlineCallbacks
    def op_declare_topic_tree(self, content, headers, msg):
        try:
            xs_name = content['exchange_space_name']
            tt_name = content['topic_tree_name']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, {'value': estr})
            return

        rc = self.declare_topic_tree(xs_name, tt_name)
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
            return

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
        log.debug('Calling EMS to create topic tree "%s/%s"' % (exchange_space_name, topic_tree_name))
        rc = yield self.ems.create_exchangename(topic_tree_name, 'New topic tree', exchange_space_name)
        if rc == None:
            log.error('Error in creating exchange name (topic tree')
            return
        log.debug('EMS returned "%s"' % str(rc))

        log.debug('Writing topic tree into registry')
        # Now need to write new topic tree into registry
        ttree = yield self.rclient.create_instance(TT_TYPE, name=topic_tree_name, description='New topic tree')

        ttree.exchange_space_name = exchange_space_name
        ttree.topic_tree_name = topic_tree_name

        log.debug('About to push topic tree into registry')
        yield self.rclient.put_instance(ttree, 'declare_topic_tree')
        log.debug('Wrote TT, id is %s' % ttree.ResourceIdentity)
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


        cstr = "%s/%s" % (topic_tree_id, topic_name)
        dset = yield self.rclient.create_instance(DSET_TYPE, name=topic_name, description=cstr)

        dset.open_dap = topic_name
        now = time.time()
        dset.last_updated = now
        dset.date_created = now
        dset.creator.name = 'Otto Niemand'
        log.debug('Dataset object created, pushing/committing "%s"' % cstr)

        yield self.rclient.put_instance(dset, 'Adding dataset/topic %s' % cstr)
        log.debug('Commit completed, %s' % dset.ResourceIdentity)
        defer.returnValue(dset.ResourceIdentity)


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

class PubSubClient(ServiceClient):
    """
    @brief Refactor of client for new interfaces
    @see http://oceanobservatories.org/spaces/display/CIDev/Pubsub+controller
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "pubsub"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def declare_topic_tree(self, exchange_space_name, topic_tree_name):
        """
        @brief Create a topic tree
        @param exchange_space_name Exchange space where the tree will live
        @param topic_tree_name Name of the tree to create
        @retval Topic tree ID on success, None if failure
        """
        yield self._check_init()
        payload = {'exchange_space_name' : exchange_space_name,
        'topic_tree_name': topic_tree_name}
        (content, headers, msg) = yield self.rpc_send('declare_topic_tree', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def undeclare_topic_tree(self, topic_tree_id):
        """
        @brief Remove a topic tree
        @param topic_tree_id ID, as returned from declare_topic_tree
        @retval None
        """
        yield self._check_init()
        payload = {'topic_tree_id' : topic_tree_id}
        (content, headers, payload) = yield self.rpc_send('undeclare_topic_tree', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def query_topic_trees(self, topic_regex):
        """
        @brief Registry query, return all trees that match the regex
        @param topic_regex Regular expression to match against
        @retval List, possibly empty, of topic tree names
        """
        yield self._check_init()
        payload = {'topic_regex' : topic_regex}
        (content, headers, payload) = yield self.rpc_send('query_topic_trees', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def define_topic(self, topic_tree_id, topic_name):
        """
        @brief Within a topic tree, define a topic. Usually a dataset name by convention.
        @param topic_tree_id ID, as returned from op_declare_topic_tree
        @param topic_name Name to declare
        @retval Topic ID, or None if error
        """
        yield self._check_init()
        payload = {'topic_tree_id' : topic_tree_id,
                'topic_name' : topic_name}
        (content, headers, payload) = yield self.rpc_send('define_topic', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def query_topics(self, exchange_point_name, topic_regex):
        """
        @brief Query topics within an exchange point
        @param exchange_point_name Exchange point to inspect (scope)
        @param topic_regex Regex to match
        @retval List, possibly empty, of topic names
        """
        yield self._check_init()
        payload = {'topic_regex' : topic_regex,
                'exchange_point_name' : exchange_point_name}
        (content, headers, payload) = yield self.rpc_send('query_topics', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def define_publisher(self, topic_tree_id, topic_id, publisher_name, credentials=None):
        """
        @brief Called by the publisher, this drops through to the resource registry
        @param topic_tree_id Tree where we'll publish
        @param publisher_name Human-readable publisher ID string, e.g. "Doc X's buoy data for NYC harbor"
        @param credentials Unused hook for auth*
        @retval Transciever instance with send() method hooked up to correct topic
        """
        yield self._check_init()
        payload = {'topic_tree_id' : topic_tree_id,
                'topic_id': topic_id,
                'publisher_name' : publisher_name,
                'credentials': credentials}
        (content, headers, payload) = yield self.rpc_send('define_publisher', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def subscribe(self, xs_name, tt_name, topic_regex):
        """
        @brief Called by subscribers, this calls the EMS to setup the data flow
        @param xs_name Exchange space name
        @param tt_name Topic tree name
        @param topic_regex Topic of interest. If no publishers, then no data, but no error
        @note Order of calls on publish/subscribe does not matter
        @note creates the queue via EMS
        @retval Address of queue for ondata() callback and subscription id
        """
        yield self._check_init()
        payload = {'topic_regex' : topic_regex,
                'exchange_space_name': xs_name,
                'topic_tree_name' : tt_name}
        (content, headers, payload) = yield self.rpc_send('subscribe', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])

    @defer.inlineCallbacks
    def unsubscribe(self, subscription_id):
        """
        @brief Remove subscription
        @param subscription_id ID from subscribe call
        @retval OK if no problems, error otherwise
        """
        yield self._check_init()
        payload = {'subscription_id':subscription_id}
        (content, headers, payload) = yield self.rpc_send('unsubscribe', payload)
        log.debug('retval: %s ' % content['value'])
        defer.returnValue(content['value'])


# Spawn off the process using the module name
factory = ProcessFactory(PubSubService)
