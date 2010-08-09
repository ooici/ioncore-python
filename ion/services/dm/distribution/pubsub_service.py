#!/usr/bin/env python

"""
@file ion/services/dm/distribution/pubsub_service.py
@author Michael Meisinger
@author David Stuebe
@brief service for publishing on data streams, and for subscribing to streams.
The service includes methods for defining topics, defining publishers, publishing,
and defining subscriptions.
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer

from ion.core import bootstrap
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.resources import dm_resource_descriptions

from ion.data import dataobject
from ion.services.dm.distribution import pubsub_registry

import ion.util.procutils as pu
from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.services.dm.util import dap_tools 
from pydap.model import DatasetType

class DataPubsubService(BaseService):
    """
    @Brief Service for Publicaiton and Subscription to topics
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
  
        if topic.RegistryIdentity:
            pass # Just change the keywords in the registry - nothing to do!
        else:
            # it is a new topic and must be declared
            topic = yield self.create_and_declare_topic(topic)
    
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', topic: \n' + str(topic))
    
        topic = yield self.reg.register(topic)
        
        #@todo call some process to update all the subscriptions? Or only on interval?
        
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
        
        
    @defer.inlineCallbacks
    def op_define_subscription(self, content, headers, msg):
        """Service operation: Register a subscriber's intent to receive
        subscriptions on a topic, with the workflow described to produce the
        desired result
        """
        # Subscribe should decouple the exchange point where data is published
        # and the subscription exchange point where a process receives it.
        # That allows for an intermediary process to filter it.
        
        logging.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' +  str(headers))
        subscription = dataobject.Resource.decode(content)
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', subscription: \n' + str(subscription))
    
        
        if subscription.RegistryIdentity:
            # it is an existing topic and must be updated
            subscription = yield self.update_subscription(subscription)
        else:
            subscription = yield self.create_subscription(subscription)
    
        subscription = yield self.reg.register(subscription)
        if subscription:
            logging.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            yield self.reply_ok(msg, subscription.encode())
        else:
            logging.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)

    @defer.inlineCallbacks        
    def create_subscription(subscription):
        '''
        '''
        #A for each topic1, topic2, topic3 get the list of queues
            
        topics={'topic1':[],'topic2':[],'topic3':[]}
            
            
        if subscription.topic1.name or subscription.topic1.keywords:
            topics['topic1'] = yield self.find_topics(subscription.topic1)
            
        if subscription.topic2.name or subscription.topic2.keywords:
            topics['topic2'] = yield self.find_topics(subscription.topic2)
        
        if subscription.topic3.name or subscription.topic3.keywords:
            topics['topic3'] = yield self.find_topics(subscription.topic3)
            
            
        #B process the workflow dictionary,
        '''
        <consumer name>:{'module':'path.to.module','cosumeclass':'<ConsumerClassName>',\
            'attach':<topicX> or (consumer name, consumer queue keyword) or <list of consumers and topics>,\
            'Process Parameters':{<conumser property keyword arg>: <property value>},\
        '''
        
        #0) Check for valid workflow
        # how can I check whether the workflow is a loop ? difficult!
                
        # 1) create new queues for workflow
        # 2) create ConsumerDesc for each consumer
        
        # for each consumer and it attachements...
        for consumer, args in subscription.workflow.items():
            
            #examine attach args an make sure they are valid 
            if hasattr(args.attach,'__iter__'):
                attach = args.attach
            else:
                attach = [args.attach]
                
            for name in attach:
                
                # is it a topic?
                if name in topics.keys():
                    topic_name = topics[name].name # get the registered topic name
                    logging.debug('Consumer %s attaches to topic %s' % (consumer, topic_name))

                # See if it is consuming another resultant
                elif name[0] in subscription.workflow.keys(): # Could fail badly!
                    
                    
                    producer = subscription.workflow[name[0]]
                    if name[1] in producer['delivery queues'].itervalues():
                        queue_arg = producer['delivery queues'][name[1]]
                        logging.debug('Consumer %s attaches to producer %s delivery queue %s'\
                            % (consumer, name[0],queue_arg))
                    
                    
        
        # 3) spawn consumers
        
        #C add the queues, ConsumerDesc's and topics to the subscription definition
        
        
        
        
    #@defer.inlineCallbacks        
    def update_subscription(subscription):
        '''
        '''
        # Determine the difference between the current and existing subscription
        
        # act accordingly ????
        pass   
        
        
        
        
        
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
        logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publication')
        #logging.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publication: \n' + str(publication))
        
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
                logging.debug(self.__class__.__name__ + '; Publishing to topic: \n' + str(topic))
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
    @Brief Client class for accessing the data pubsub service.
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
        assert isinstance(topic, dataobject.Resource), 'Invalid argument to define_topic'
        
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
        assert isinstance(publisher, dm_resource_descriptions.PublisherResource), 'Invalid argument to define_publisher'
        

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
    def define_subscription(self, subscription):
        """
        @Brief define and register a subscription, or update existing
        """

        logging.info(self.__class__.__name__ + '; Calling: define_subscription')
        assert isinstance(subscription, dm_resource_descriptions.SubscriptionResource), 'Invalid argument to define_subscription'
        

        (content, headers, msg) = yield self.rpc_send('define_subscription',
                                            publisher.encode())
        logging.debug(self.__class__.__name__ + ': define_subscription; Result:' + str(headers))
        
        if content['status']=='OK':
            logging.info(self.__class__.__name__ + '; define_subscription: Success!')
            publisher = dataobject.Resource.decode(content['value'])
            defer.returnValue(publisher)
        else:
            logging.info(self.__class__.__name__ + '; define_subscription: Failed!')
            defer.returnValue(None)

