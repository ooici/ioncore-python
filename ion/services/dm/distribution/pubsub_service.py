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
log = ion.util.ionlog.getLogger(__name__)

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
from ion.services.dm.distribution import base_consumer


class DataPubsubService(BaseService):
    """
    @brief Service for Publication and Subscription to topics
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

        log.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' + str(headers))
        topic = dataobject.Resource.decode(content)

        if topic.RegistryIdentity:
            yield self.update_topic_registration(topic)
        else:
            # it is a new topic and must be declared
            topic = yield self.create_and_register_topic(topic)

        log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', topic: \n' + str(topic))

        #@todo call some process to update all the subscriptions? Or only on interval?
        if topic:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            yield self.reply_ok(msg, topic.encode())
        else:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)

    #@defer.inlineCallback # call back not needed
    def update_topic_registration(self,topic):
        #topic = yield self.reg.register(topic)
        return self.reg.register(topic)


    @defer.inlineCallbacks
    def create_and_register_topic(self,topic):
        """
        Create a messaging name and set the queue properties to create it.
        @todo fix the hack - This should come from the exchange registry!
        """
        log.info(self.__class__.__name__ + '; Declaring new Topic & Creating Queue.')
        # Give the topic anidentity
        topic.create_new_reference()

        # Declare the queue - this should be done in the exchange registry
        # Create a new queue object
        queue = yield self.create_queue()

        topic.queue = queue

        topic = yield self.reg.register(topic)

        defer.returnValue(topic)

    @defer.inlineCallbacks
    def create_queue(self):
        """
        Create a queue
        @todo fix the hack - This should come from the exchange registry!
        """

        queue = dm_resource_descriptions.Queue()
        queue.name = dataobject.create_unique_identity()
        queue.type = 'fanout'
        queue.args = {'scope':'global'}

        queue_properties = {queue.name:{'name_type':queue.type, 'args':queue.args}}
        # This should come from the COI Exchange registry
        yield bootstrap.declare_messaging(queue_properties)

        defer.returnValue(queue)



    @defer.inlineCallbacks
    def op_define_publisher(self, content, headers, msg):
        """Service operation: Register a publisher that subsequently is
        authorized to publish on a topic.
        """
        log.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' +  str(headers))
        publisher = dataobject.Resource.decode(content)
        log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publisher: \n' + str(publisher))

        publisher = yield self.reg.register(publisher)
        if publisher:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            yield self.reply_ok(msg, publisher.encode())
        else:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
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

        log.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' +  str(headers))
        subscription = dataobject.Resource.decode(content)
        log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', subscription: \n' + str(subscription))


        if subscription.RegistryIdentity:
            # it is an existing topic and must be updated
            subscription = yield self.update_subscription(subscription)
        else:
            subscription = yield self.create_subscription(subscription)

        subscription = yield self.reg.register(subscription)
        if subscription:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
            yield self.reply_ok(msg, subscription.encode())
        else:
            log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Failed!')
            yield self.reply_err(msg, None)

    """
    Subscription Object:
    Name - inherited
    # hack for now to allow naming one-three more topic descriptions
    topic1 = TypedAttribute(PubSubTopicResource)
    topic2 = TypedAttribute(PubSubTopicResource)
    topic3 = TypedAttribute(PubSubTopicResource)

    workflow = TypedAttribute(dict)
    '''
    <consumer name>:{'module':'path.to.module','cosumeclass':'<ConsumerClassName>',\
        'attach':(<topicX>) or (<consumer name>, <consumer queue keyword>) or <list of consumers and topics>,\
        'Process Parameters':{<conumser property keyword arg>: <property value>}}
    '''
    """

    @defer.inlineCallbacks
    def create_subscription(self,subscription):
        '''
        '''
        subscription.create_new_reference()

        subscription = yield self.create_consumer_args(subscription)

        consumer_args = subscription.consumer_args

        for name, args in consumer_args.items():
            child = base_consumer.ConsumerDesc(**args)
            child_id = yield self.spawn_child(child)
            #subscription.consumer_procids[name]=child_id


        subscription = yield self.reg.register(subscription)

        defer.returnValue(subscription)

    @defer.inlineCallbacks
    def create_consumer_args(self,subscription):
        '''
        @brief Turn the workflow argument into the arguments used to create a
        new consumer. This includes spawning the queues needed to deliver the
        product of one consumer to the attachment point of another!
        @note This is probably too complex - it should be refactored into seperate
        methods where possible.
        '''

        log.info('Processing Subscription Workflow:'+str(subscription.workflow))

        #A for each topic1, topic2, topic3 get the list of queues

        topics={'topic1':[],'topic2':[],'topic3':[]}


        if subscription.topic1.name or subscription.topic1.keywords:
            topics['topic1'] = yield self.find_topics(subscription.topic1)

        if subscription.topic2.name or subscription.topic2.keywords:
            topics['topic2'] = yield self.find_topics(subscription.topic2)

        if subscription.topic3.name or subscription.topic3.keywords:
            topics['topic3'] = yield self.find_topics(subscription.topic3)


        #B process the workflow dictionary,


        #0) Check for valid workflow
        # how can I check whether the workflow is a loop ? difficult!

        # 1) create new queues for workflow
        # 2) create ConsumerDesc for each consumer

        # create place holders for each consumers spawn args
        subscription_queues=[]
        consumers = {}
        consumer_names =  subscription.workflow.keys()
        for consumer, args in subscription.workflow.items():

            spargs ={'attach':[],
                     'process parameters':args.get('process parameters',{}),
                     'delivery interval':args.get('delivery interval',None),
                     'delivery queues':args.get('delivery queues',{})}

            for k,v in spargs['delivery queues']:
                topic = topics.get(v,None)
                if not topic:
                    raise RuntimeError('Invalid delivery queue specified ')
                spargs['delivery queues'][k]=topic.queue.name


            cd = {}
            cd['name'] = str(consumer)
            cd['module'] = str(args.get('module'))
            cd['procclass'] = str(args.get('consumerclass'))
            cd['spawnargs'] = spargs

            consumers[consumer]=cd

        #log.info('CONSUMERS:' + str(consumers))

        # for each consumer and add its attachements and create delivery queues
        for consumer, args in subscription.workflow.items():

            # list of queues to attach to for this consumer
            attach = []

            # Get the attach to topic or consumer - make it a list and iterate
            attach_to= args['attach']
            if not hasattr(attach_to,'__iter__'):
                attach_to = [attach_to]

            for item in attach_to:

                #Each item may be a topic or a consumer/keyword pair - make it a list
                # and extract the contents
                if not hasattr(item,'__iter__'):
                    item = [item]

                if len(item) ==1:
                    name = item[0]
                    keyword = None
                elif len(item)==2:
                    name = item[0]
                    keyword = item[1]
                else:
                    raise RuntimeError('Invalid attach argument!')


                # is it a topic?
                if name in topics.keys():
                    topic_list = topics[name] # get the list of topics
                    # add each queue to the list of attach_to
                    for topic in topic_list:
                        # Add it to the list for this consumer
                        attach.append(topic.queue.name)
                        # Add it to the list for this subscription
                        subscription_queues.append(topic.queue)
                        log.info('''Consumer '%s' attaches to topic name '%s' ''' % (consumer, topic.name))

                # See if it is consuming another resultant
                elif name in consumer_names: # Could fail badly!

                    # Does this producer already have a queue?
                    # This is not 'safe' - lots of ways to get a key error!
                    q = consumers[name]['spawnargs']['delivery queues'].get(keyword,None)
                    if q:
                        attach.append(q)
                        log.info('''Consumer '%s' attaches to existing queue for producer/keyword: '%s'/'%s' ''' % (consumer, name, keyword))
                    else:
                        # Create the queue!
                        #@todo - replace with call to exchange registry service
                        queue = yield self.create_queue()
                        # Add it to the list for this consumer
                        attach.append(queue.name)
                        # Add it to the list for this subscription
                        subscription_queues.append(queue)

                        # Add to the delivery list for the producer...
                        consumers[name]['spawnargs']['delivery queues'][keyword]=str(queue.name)
                        log.info('''Consumer '%s' attaches to new queue for producer/keyword: '%s'/'%s' ''' % (consumer, name, keyword))
                else:
                    raise RuntimeError('''Can not determine how to attach consumer '%s' \
                                       to topic or consumer '%s' ''' % (consumer, item))

            consumers[consumer]['spawnargs']['attach'].extend(attach)

        log.info('CONSUMERS:' + str(consumers))

        subscription.queues = subscription_queues
        subscription.consumer_args = consumers

        defer.returnValue(subscription)




    #@defer.inlineCallbacks
    def update_subscription(subscription):
        '''
        '''
        # Determine the difference between the current and existing subscription

        # act accordingly - but very difficult to figure out what to do!
        raise RuntimeError('Update Subscription is not yet implemented')
        pass






    def op_unsubscribe(self, content, headers, msg):
        """Service operation: Stop one's existing subscription to a topic.
            And remove the Queue if no one else is listening...
        """

    @defer.inlineCallbacks
    def op_publish(self, content, headers, msg):
        """Service operation: Publish data message on a topic
        """
        log.debug(self.__class__.__name__ +', op_'+ headers['op'] +' Received: ' +  str(headers))
        publication = dataobject.Resource.decode(content)
        log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publication')
        #log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publication: \n' + str(publication))

        #Get the data
        data = publication.data

        # Get the Topic
        topic_ref = publication.topic_ref
        topic = yield self.reg.get(topic_ref.reference(head=True))
        if not topic:
            log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', topic invalid!')
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
                log.debug(self.__class__.__name__ + '; Publishing to topic: \n' + str(topic))
                break

        if not valid:
            log.info(self.__class__.__name__ + ' recieved: op_'+ headers['op'] +', publisher not registered for topic!')
            yield self.reply_err(msg, 'Publisher not registered for topic!')
            return

        if not data.notification:
            data.notification = 'Data topic: ' + topic.name

        data.timestamp = pu.currenttime()

        # Todo: impersonate message as from sender -
        #@todo - Move the actual publish command back to the client side!
        yield self.send(topic.queue.name, 'data', data.encode(), {})

        log.info(self.__class__.__name__ + ': op_'+ headers['op'] + ' Success!')
        yield self.reply_ok(msg, '')

    #@defer.inlineCallbacks
    def find_topics(self, topic_description):
        """Service operation: For a given resource, find the topic that contains
        updates to the resource or resource description.
        @note - should this create a topic if none yet exist?
        """

        #Note - AOI does not do anything yet and keyword needs to be improved...
        #topic_list = yield self.reg.find(topic_description,regex=True, attnames=['name','keywords','aoi'])
        return self.reg.find(topic_description,regex=True, attnames=['name','keywords','aoi'])



# Spawn of the process using the module name
factory = ProtocolFactory(DataPubsubService)


class DataPubsubClient(BaseServiceClient):
    """
    @brief Client class for accessing the data pubsub service.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_pubsub"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def define_topic(self, topic):
        """
        @brief Define and register a topic, creating the exchange queue, or update
        existing properties.
        @note All business logic associated with defining a topic has been moved to the service
        """
        yield self._check_init()

        log.info(self.__class__.__name__ + '; Calling: define_topic')
        assert isinstance(topic, dataobject.Resource), 'Invalid argument to define_topic'

        (content, headers, msg) = yield self.rpc_send('define_topic',
                                            topic.encode())
        log.debug(self.__class__.__name__ + ': define_topic; Result:' + str(headers))

        if content['status']=='OK':
            topic = dataobject.Resource.decode(content['value'])
            log.info(self.__class__.__name__ + '; define_topic: Success!')
            defer.returnValue(topic)
        else:
            log.info(self.__class__.__name__ + '; define_topic: Failed!')
            defer.returnValue(None)


    @defer.inlineCallbacks
    def define_publisher(self, publisher):
        """
        @brief define and register a publisher, or update existing
        """
        yield self._check_init()

        log.info(self.__class__.__name__ + '; Calling: define_publisher')
        assert isinstance(publisher, dm_resource_descriptions.PublisherResource), 'Invalid argument to define_publisher'


        (content, headers, msg) = yield self.rpc_send('define_publisher',
                                            publisher.encode())
        log.debug(self.__class__.__name__ + ': define_publisher; Result:' + str(headers))

        if content['status']=='OK':
            log.info(self.__class__.__name__ + '; define_publisher: Success!')
            publisher = dataobject.Resource.decode(content['value'])
            defer.returnValue(publisher)
        else:
            log.info(self.__class__.__name__ + '; define_publisher: Failed!')
            defer.returnValue(None)


    @defer.inlineCallbacks
    def publish(self, publisher_proc, topic_ref, data):
        """
        @brief Publish something...
        @param publisher_proc - the publishing process passes self to the publish client
        @param topic_ref is a topic reference for which the publisher is registered for (checked before sending)
        @param data is a dataobject which is being published

        @todo move the actual publish command back to this client! Don't send the data
        to the service!
        """
        yield self._check_init()

        log.info(self.__class__.__name__ + '; Calling: publish')

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
            log.info('%s; publish: Failed! Invalid DataType: %s' % (self.__class__.__name__, type(data)))
            raise RuntimeError('%s; publish: Invalid DataType: %s' % (self.__class__.__name__, type(data)))

        publication.data = do
        publication.topic_ref = topic_ref
        publication.publisher = publisher_proc.receiver.spawned.id.full

        (content, headers, msg) = yield self.rpc_send('publish',
                                            publication.encode())
        log.debug(self.__class__.__name__ + ': publish; Result:' + str(headers))

        if content['status']=='OK':
            log.info(self.__class__.__name__ + '; publish: Success!')
            defer.returnValue('sent')
        else:
            log.info(self.__class__.__name__ + '; publish: Failed!')
            defer.returnValue('error sending message!')


    @defer.inlineCallbacks
    def define_subscription(self, subscription):
        """
        @brief define and register a subscription, or update existing
        """
        yield self._check_init()

        log.info(self.__class__.__name__ + '; Calling: define_subscription')
        assert isinstance(subscription, dm_resource_descriptions.SubscriptionResource), 'Invalid argument to define_subscription'


        (content, headers, msg) = yield self.rpc_send('define_subscription',
                                            subscription.encode())
        log.debug(self.__class__.__name__ + ': define_subscription; Result:' + str(headers))

        if content['status']=='OK':
            log.info(self.__class__.__name__ + '; define_subscription: Success!')
            subscription = dataobject.Resource.decode(content['value'])
            defer.returnValue(subscription)
        else:
            log.info(self.__class__.__name__ + '; define_subscription: Failed!')
            defer.returnValue(None)
