#!/usr/bin/env python

"""
@file ion/services/dm/datapubsub.py
@author Michael Meisinger
@brief service for publishing on data streams, and for subscribing to streams
"""


from twisted.internet import defer

from ion.data import dataobject
from ion.data.datastore import registry
from ion.data import store

from ion.core import bootstrap
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient




class DataPubSubRegistryService(registry.BaseRegistryService):
    """
    A very simple registry for Data Pub Sub
    @TODO make the interface more specific for different kinds of pubsub objects
    Need to specify topic, publisher, subscriber
    """
 
     # Declaration of service
    declare = BaseService.service_declare(name='datapubsub_registry', version='0.1.0', dependencies=[])

    op_clear_registry = registry.BaseRegistryService.base_clear_registry
    """
    Service operation: clear registry
    """
    op_register = registry.BaseRegistryService.base_register_resource
    """
    Service operation: Create or update a pubsub.
    """    
    op_get = registry.BaseRegistryService.base_get_resource
    """
    Service operation: Get pubsub
    """
    op_find = registry.BaseRegistryService.base_find_resource
    """
    Service operation: Find pubsub by characteristics
    """


        
# Spawn of the process using the module name
factory = ProtocolFactory(DatasetRegistryService)


class DataPubSubRegistryClient(registry.BaseRegistryClient):
    """
    Class for the client accessing the Data PubSub Registry.
    @Todo clean up the interface for specific pubsub resource objects
    Need to specify topic, publisher, subscriber
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "datapubsub_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_registry')


    def register(self,pubsub):
        """
        @Brief Client method to Register Pubsub resources
        @param dataset is an instance of a dataset resource
        """
        return  self.base_register_resource('register', pubsub)    

    
    def get(self,pubsub_reference):
        """
        @Brief Get a pubsub resource by reference
        @param dpubsub_resource_reference is the unique reference object for a registered resource
        """
        return self.base_get_resource('get',pubsub_reference)
        
    def find(self, description,regex=True,ignore_defaults=True,attnames=[]):
        """
        @Brief find all registered datasets which match the attributes of description
        @param see the registry docs for params
        """
        return self.base_find_resource('find',description,regex,ignore_defaults,attnames)




class DataPubsubService(BaseService):
    """Data publish/subscribe service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_pubsub',
                                          version='0.1.0',
                                          dependencies=[])
    @defer.inlineCallbacks
    def slc_init(self):
        self.topics = yield Store.create_store()
        #self.reg = DataPubSubRegistryClient(sup)
        

    @defer.inlineCallbacks
    def op_define_topic(self, content, headers, msg):
        """Service operation: Register a "topic" that can be published on and
        that can be subscribed to. Note: this has no direct connection to any
        AMQP topic notion. A topic is basically a data stream.
        """
        topic_name = content['topic_name']
        topic = {topic_name:{'name_type':'fanout', 'args':{'scope':'system'}}}
        yield bootstrap.declare_messaging(topic)
        qtopic_name = self.get_scoped_name('system',topic_name)
        yield self.topics.put(topic_name, topic[topic_name])
        yield self.reply_ok(msg, {'topic_name':qtopic_name}, {})

    def op_define_publisher(self, content, headers, msg):
        """Service operation: Register a publisher that subsequently is
        authorized to publish on a topic.
        """

    def op_subscribe(self, content, headers, msg):
        """Service operation: Register a subscriber's intent to receive
        subscriptions on a topic, with additional filter and delivery method
        details.
        """
        subscriber = None
        topic = None
        eventOnly = False

    def op_unsubscribe(self, content, headers, msg):
        """Service operation: Stop one's existing subscription to a topic.
        """

    @defer.inlineCallbacks
    def op_publish(self, content, headers, msg):
        """Service operation: Publish data message on a topic
        """
        topic_name = content['topic_name']
        headers = content['msg_headers']
        op = content['msg_op']
        msg = content['msg']
        qtopic = self.get_scoped_name('system',topic_name)
        # Todo: impersonate message as from sender
        yield self.send(qtopic, op, msg, headers)

    def find_topic(self, content, headers, msg):
        """Service operation: For a given resource, find the topic that contains
        updates to the resource or resource description. Might involve creation
        of this topic of this topic does not yet exist
        """

# Spawn of the process using the module name
factory = ProtocolFactory(DataPubsubService)


class DataPubsubClient(BaseServiceClient):
    """
    Client class for accessing the data pubsub service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_pubsub"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_topic(self, topic_name):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('define_topic',
                                        {'topic_name':topic_name}, {})
        defer.returnValue(str(content['topic_name']))

    @defer.inlineCallbacks
    def subscribe(self, topic_name):
        pass
