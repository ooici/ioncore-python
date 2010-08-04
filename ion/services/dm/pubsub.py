#!/usr/bin/env python

"""
@file ion/services/dm/datapubsub.py
@author Michael Meisinger
@brief service for publishing on data streams, and for subscribing to streams
@Note Should the pubsub service use the pubsub registry client or should it
implement the communication with the pubsub registry service?
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer

from ion.core import bootstrap
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.resources import dm_resource_descriptions

from ion.data import dataobject
from ion.services.dm.datapubsub import pubsub_registry

import ion.util.procutils as pu
from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.services.dm.util import dap_tools 
from pydap.model import DatasetType

class DataPubsubService(BaseService):
    """Data publish/subscribe service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='data_pubsub',
                                          version='0.1.0',
                                          dependencies=[])
    @defer.inlineCallbacks
    def slc_init(self):
        
        # Is this the proper way to start a client in a service?
        self.reg = yield pubsub_registry.DataPubsubRegistryClient(proc=self)
        

    @defer.inlineCallbacks
    def op_define_topic(self, content, headers, msg):
        """Service operation: Register a "topic" that can be published on and
        that can be subscribed to. Note: this has no direct connection to any
        AMQP topic notion. A topic is basically a data stream.
        """
        
        logging.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' + str(headers))
        topic = dataobject.Resource.decode(content)
  
        if not topic.RegistryIdentity:
            # it is a new topic and must be declared
            topic = yield self.create_and_declare_topic(topic)
        # Otherwise assume we are updating the keywords or some such change
        # That does not affect the queue!
    
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', topic: \n' + str(topic))
    
        topic = yield self.reg.register(topic)
        if topic:
            logging.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            yield self.reply_ok(msg, topic.encode())
        else:
            logging.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks
    def create_and_declare_topic(self,topic):
        """
        Create a messaging name and set the queue properties to create it.
        @TODO fix the hack - This should come from the exchange registry!
        """
        logging.info(self.__class__.__name__ + '; Declaring new Topic & Creating Queue.')
        # Give the topic anidentity
        topic.create_new_reference()
        
        # Pick a topic name and declare the queue - this should be done in the exchange registry        
        # Create a new queue object
        queue = dm_resource_descriptions.Queue()
        queue.name = dataobject.create_unique_identity()
        queue.type = 'fanout'
        queue.args = {'scope':'global'}
        
        queue_properties = {queue.name:{'name_type':queue.type, 'args':queue.args}}
        # This should come from the COI Exchange registry
        yield bootstrap.declare_messaging(queue_properties)

        topic.queue = queue

        defer.returnValue(topic)

    @defer.inlineCallbacks
    def op_define_publisher(self, content, headers, msg):
        """Service operation: Register a publisher that subsequently is
        authorized to publish on a topic.
        """
        logging.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' +  str(headers))
        publisher = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publisher: \n' + str(publisher))
    
        publisher = yield self.reg.register(publisher)
        if publisher:
            logging.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            yield self.reply_ok(msg, publisher.encode())
        else:
            logging.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)
        
        
    
    def op_subscribe(self, content, headers, msg):
        """Service operation: Register a subscriber's intent to receive
        subscriptions on a topic, with additional filter and delivery method
        details.
        """
        # Subscribe should decouple the exchange point where data is published
        # and the subscription exchange point where a process receives it.
        # That allows for an intermediary process to filter it.
        
        subscriber = None
        topic = None
        eventOnly = False

    def op_unsubscribe(self, content, headers, msg):
        """Service operation: Stop one's existing subscription to a topic.
            And remove the Queue if no one else is listening...
        """
        
    @defer.inlineCallbacks
    def op_publish(self, content, headers, msg):
        """Service operation: Publish data message on a topic
        """
        logging.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' +  str(headers))
        publication = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publication: \n' + str(publication))
        
        #Get the data
        data = publication.data
        
        # Get the Topic
        topic_ref = publication.topic_ref
        topic = yield self.reg.get(topic_ref.reference(head=True))
        if not topic:
            logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', topic invalid!')
            yield self.reply_err(msg, 'Topic does not exist')
            return
        
        #Check publisher is valid!
        publisher = dm_resource_descriptions.PublisherResource()
        publisher.publisher = publication.publisher
        
        # Get the publications which this process is registered fro
        reg_pubs = yield self.reg.find(publisher, regex=False, attnames=['publisher'])
        
        valid = False
        for pub in reg_pubs:
            
            if topic.reference(head=True) in pub.topics:
                valid = True
                logging.info(self.__class__.__name__ + '; Publishing to topic: \n' + str(topic))
                break
        
        if not valid:
            logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publisher not registered for topic!')
            yield self.reply_err(msg, 'Publisher not registered for topic!')
            return
        
        if not data.notification:
            data.notification = 'Data topic: ' + topic.name
        
        data.timestamp = pu.currenttime()
        
        # Todo: impersonate message as from sender
        yield self.send(topic.queue.name, 'data', data.encode(), {})

        logging.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
        yield self.reply_ok(msg, '')
        
    
    def find_topic(self, content, headers, msg):
        """Service operation: For a given resource, find the topic that contains
        updates to the resource or resource description. Might involve creation
        of this topic if this topic does not yet exist
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
    def define_topic(self, topic):
        """
        @Brief Define and register a topic, creating the exchange queue, or update
        existing properties.
        @Note All business logic associated with defining a topic has been moved to the service
        """
        
        logging.info(self.__class__.__name__ + '; Calling: define_topic')
        assert isinstance(topic, dataobject.Resource), 'Invalid argument to base_register_resource'
        
        (content, headers, msg) = yield self.rpc_send('define_topic',
                                            topic.encode())
        logging.debug(self.__class__.__name__ + ': define_topic; Result:' + str(headers))
        
        if content['status']=='OK':
            topic = dataobject.Resource.decode(content['value'])
            logging.info(self.__class__.__name__ + '; define_topic: Success!')
            defer.returnValue(topic)
        else:
            logging.info(self.__class__.__name__ + '; define_topic: Failed!')
            defer.returnValue(None)


    @defer.inlineCallbacks
    def define_publisher(self, publisher):
        """
        @Brief define and register a publisher, or update existing
        """

        logging.info(self.__class__.__name__ + '; Calling: define_publisher')
        assert isinstance(publisher, dm_resource_descriptions.PublisherResource), 'Invalid argument to base_register_resource'
        

        (content, headers, msg) = yield self.rpc_send('define_publisher',
                                            publisher.encode())
        logging.debug(self.__class__.__name__ + ': define_publisher; Result:' + str(headers))
        
        if content['status']=='OK':
            logging.info(self.__class__.__name__ + '; define_publisher: Success!')
            publisher = dataobject.Resource.decode(content['value'])
            defer.returnValue(publisher)
        else:
            logging.info(self.__class__.__name__ + '; define_publisher: Failed!')
            defer.returnValue(None)

    
    @defer.inlineCallbacks
    def publish(self, publisher_proc, topic_ref, data):
        """
        @Brief Publish something...
        @param publisher_proc - the publishing process passes self to the publish client
        @param topic_ref is a topic reference for which the publisher is registered for (checked before sending)
        @param data is a dataobject which is being published
        """
        logging.info(self.__class__.__name__ + '; Calling: publish')
        
        publication = dm_resource_descriptions.Publication()
        
        #Load the args and pass to the publisher
        if isinstance(data, dm_resource_descriptions.DataMessageObject):
            do = data
        elif isinstance(data, DatasetType):
            do = dap_tools.ds2dap_msg(data)
        elif isinstance(data, dict):
            do = dm_resource_descriptions.DictionaryMessageObject()
            do.data=data
        elif isinstance(data, str):
            do = dm_resource_descriptions.StringMessageObject()
            do.data=data
        else:
            logging.info('%s; publish: Failed! Invalid DataType: %s' % (self.__class__.__name__, type(data)))
            raise RuntimeError('%s; publish: Invalid DataType: %s' % (self.__class__.__name__, type(data)))
        
        publication.data = do
        publication.topic_ref = topic_ref
        publication.publisher = publisher_proc.receiver.spawned.id.full
        
        (content, headers, msg) = yield self.rpc_send('publish',
                                            publication.encode())
        logging.debug(self.__class__.__name__ + ': publish; Result:' + str(headers))
        
        if content['status']=='OK':
            logging.info(self.__class__.__name__ + '; publish: Success!')
            defer.returnValue('sent')
        else:
            logging.info(self.__class__.__name__ + '; publish: Failed!')
            defer.returnValue('error sending message!')


    @defer.inlineCallbacks
    def subscribe(self, subscription_name='', identity=None, select_on={}, workflow=(), delivery='', deliver_to=None, notification={}):
        """
        subscription_name - the name of this subscription, Should be unique to your subscriptions
        identity - ResourceReference for your OOI identity
        select_on - a dictionary describing the topics to find and register for
            {'name':'<regex>','keyword':'<regex>','AOI':<Not Yet!>}
        workflow - a tuple of consumer methods to process the data
        deliver - digest 
        
        workflow=[{name:consumer1,
                    class:path.module,
                    args:{<queuearg>:consumername,...kwargs}},
                   {name:consumer2 ...)
                   
        delivery='asap' or 'digest'
        deliver_to - A topic to publish the results on or None
        notification - {'twitter':'<params>'}, {'email':'<params>'}, {'sms':'<params>'}, {'rss':'<params>'}
        """
        pass
