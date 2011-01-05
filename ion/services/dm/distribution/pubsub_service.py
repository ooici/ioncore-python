#!/usr/bin/env python

"""
@file ion/services/dm/distribution/pubsub_service.py
@package ion.services.dm.distribution.pubsub
@author Michael Meisinger
@author David Stuebe
@brief service for publishing on data streams, and for subscribing to streams.
The service includes methods for defining topics, defining publishers, publishing,
and defining subscriptions.
"""

import ion.util.ionlog

from twisted.internet import defer

from ion.core import bootstrap
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.resources import dm_resource_descriptions

from ion.data import dataobject
from ion.services.dm.distribution import pubsub_registry

import ion.util.procutils as pu
from ion.core import ioninit

# Global objects
CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)

from ion.services.dm.distribution import base_consumer

class PubSubService(ServiceProcess):
    """
    @brief Refactored pubsub service
    @see http://oceanobservatories.org/spaces/display/CIDev/Pubsub+controller
    @todo Add runtime dependency on exchange management service

    Hierarchy is
    Exchange space => exchange point => Topic tree => topic name

    e.g.
    OOICI / DM / science data / test.pydap.org:coads.nc
    """
    declare = ServiceProcess.service_declare(name='pubsub',
                                          version='0.1.0',
                                          dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        # Link to registry
        self.reg = yield pubsub_registry.DataPubsubRegistryClient(proc=self)

    # Protocol entry points. Responsible for parsing and unpacking arguments
    def op_declare_topic_tree(self, content, headers, msg):
        try:
            xs_name = content['exchange_space_name']
            tt_name = content['topic_tree_name']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            self.reply_ok(msg, {'value': estr})
            return

        rc = self.declare_topic_tree(xs_name, tt_name)
        self.reply_ok(msg, {'value': rc})

    def op_undeclare_topic_tree(self, content, headers, msg):
        return self.undeclare_topic_tree(content['topic_tree_id'])

    def op_query_topic_trees(self, content, headers, msg):
        return self.query_topic_trees(content['topic_regex'])

    def op_query_topics(self, content, headers, msg):
        return self.query_topics(content['exchange_point_name'], content['topic_regex'])

    def op_define_topic(self, content, headers, msg):
        return self.define_topic(content['topic_tree_id'], content['topic_name'])

    def op_define_publisher(self, content, headers, msg):
        return self.define_publisher(content['topic_tree_id'], content['topic_id'],
                                     content['publisher_name'], content['credentials'])

    def op_subscribe(self, content, headers, msg):
        return self.subscribe(content['topic_regex'])

    def op_unsubscribe(self, content, headers, msg):
        return self.unsubscribe(content['subscription_id'])




    #    API-style entry points
    def declare_topic_tree(self, exchange_space_name, topic_tree_name):
        """
        @brief Create a topic tree
        @param exchange_space_name Exchange space where the tree will live
        @param topic_tree_name Name of the tree to create
        @retval Topic tree ID on success, None if failure
        """
        log.error('DTT Not implemented')

    def undeclare_topic_tree(self, topic_tree_id):
        """
        @brief Remove a topic tree
        @param topic_tree_id ID, as returned from declare_topic_tree
        @retval None
        """
        log.error('Not implemented')

    def query_topic_trees(self, topic_regex):
        """
        @brief Registry query, return all trees that match the regex
        @param topic_regex Regular expression to match against
        @retval List, possibly empty, of topic tree names
        """
        log.error('Not implemented')

    def query_topics(self, exchange_point_name, topic_regex):
        """
        @brief Query topics within an exchange point
        @param exchange_point_name Exchange point to inspect (scope)
        @param topic_regex Regex to match
        @retval List, possibly empty, of topic names
        """
        log.error('Not implemented')

    def define_topic(self, topic_tree_id, topic_name):
        """
        @brief Within a topic tree, define a topic. Usually a dataset name by convention.
        @param topic_tree_id ID, as returned from op_declare_topic_tree
        @param topic_name Name to declare
        @retval Topic ID, or None if error
        """
        log.error('Not implemented')

    def define_publisher(self, topic_tree_id, topic_id, publisher_name, credentials=None):
        """
        @brief Called by the publisher, this drops through to the resource registry
        @param topic_tree_id Tree where we'll publish
        @param publisher_name Human-readable publisher ID string, e.g. "Doc X's buoy data for NYC harbor"
        @param credentials Unused hook for auth*
        @retval Transciever instance with send() method hooked up to correct topic
        """
        log.error('Not implemented')

    def subscribe(self, topic_regex):
        """
        @brief Called by subscribers, this calls the EMS to setup the data flow
        @param topic_regex Topic of interest. If no publishers, then no data, but no error
        @note Order of calls on publish/subscribe does not matter
        @note creates the queue via EMS
        @retval Address of queue for ondata() callback and resource id
        """
        log.error('Not implemented')

    def unsubscribe(self, subscription_id):
        """
        @brief Remove subscription
        @param subscription_id ID from subscribe calS
        @retval OK if no problems, error otherwise
        """
        log.error('Not implemented')

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
        yield self._check_init()
        payload = {'exchange_space_name' : exchange_space_name,
        'topic_tree_name': topic_tree_name}
        (content, headers, msg) = yield self.rpc_send('declare_topic_tree', payload)
        log.debug('retval: %s ' % content['value'])

    @defer.inlineCallbacks
    def undeclare_topic_tree(self, topic_tree_id):
        yield self._check_init()
        log.error('Not implemented')

    @defer.inlineCallbacks
    def query_topic_trees(self, topic_regex):
        yield self._check_init()
        log.error('Not implemented')

    @defer.inlineCallbacks
    def query_topics(self, exchange_point_name, topic_regex):
        yield self._check_init()
        log.error('Not implemented')

    @defer.inlineCallbacks
    def define_topic(self, topic_tree_id, topic_name):
        yield self._check_init()
        log.error('Not implemented')

    @defer.inlineCallbacks
    def define_publisher(self, topic_tree_id, topic_id, publisher_name, credentials=None):
        yield self._check_init()
        log.error('Not implemented')

    @defer.inlineCallbacks
    def subscribe(self, topic_regex):
        yield self._check_init()
        log.error('Not implemented')

    @defer.inlineCallbacks
    def unsubscribe(self, subscription_id):
        yield self._check_init()
        log.error('Not implemented')


# Spawn off the process using the module name
factory = ProcessFactory(PubSubService)
