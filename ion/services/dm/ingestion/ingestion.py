#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/ingestion.py
@author Michael Meisinger
@author David Stuebe
@author Dave Foster <dfoster@asascience.com>
@author Tim LaRocque (client changes only)
@brief service for registering resources

To test this with the Java CC!
> scripts/start-cc -h amoeba.ucsd.edu -a sysname=eoitest res/scripts/eoi_demo.py
"""

import time
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor
from twisted.python import reflect

from ion.services.coi import datastore

from ion.core.object import gpb_wrapper

from net.ooici.services.coi import resource_framework_pb2
from net.ooici.core.type import type_pb2

from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry_beta.resource_client import \
    ResourceClient
from ion.services.dm.distribution.publisher_subscriber import Subscriber

from ion.core.exception import ApplicationError

# For testing - used in the client
from net.ooici.play import addressbook_pb2
from ion.services.dm.distribution.publisher_subscriber import Publisher
from ion.services.dm.distribution.pubsub_service import PubSubClient, XS_TYPE, XP_TYPE, TOPIC_TYPE, SUBSCRIBER_TYPE


from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.core.object import object_utils

person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)

PERFORM_INGEST_TYPE           = object_utils.create_type_identifier(object_id=2002, version=1)
CREATE_DATASET_TOPICS_TYPE  = object_utils.create_type_identifier(object_id=2003, version=1)
INGESTION_READY_TYPE        = object_utils.create_type_identifier(object_id=2004, version=1)

class IngestionError(ApplicationError):
    """
    An error occured during the begin_ingest op of IngestionService.
    """
    pass

class IngestionService(ServiceProcess):
    """
    Place holder to move data between EOI and the datastore
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='ingestion', version='0.1.0', dependencies=[])

    #TypeClassType = gpb_wrapper.get_type_from_obj(type_pb2.ObjectType())

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        #assert isinstance(backend, store.IStore)
        #self.backend = backend
        ServiceProcess.__init__(self, *args, **kwargs)

        self.push = self.workbench.push
        self.pull = self.workbench.pull
        self.fetch_blobs = self.workbench.fetch_blobs
        self.op_fetch_blobs = self.workbench.op_fetch_blobs

        self._defer_ingest = defer.Deferred()       # waited on by op_ingest to signal end of ingestion

        self.mc = MessageClient(proc=self)

        self._pscclient = PubSubClient(proc=self)

        log.info('IngestionService.__init__()')

    @defer.inlineCallbacks
    def op_ingest(self, content, headers, msg):
        """
        Push this dataset to the datastore
        """
        log.debug('op_ingest recieved content:'+ str(content))

       
        msg_repo = content.Repository
        
        result = yield self.push('datastore', msg_repo)
        
        assert result.MessageResponseCode == result.ResponseCodes.OK, 'Push to datastore failed!'
        
        yield self.reply(msg, content=msg_repo.repository_key)
        


    @defer.inlineCallbacks
    def op_retrieve(self, content, headers, msg):
        """
        Return the root group of the dataset
        Content is the unique ID for a particular dataset
        """
        log.debug('op_retrieve: recieved content:'+ str(content))
        result = yield self.pull('datastore', str(content))
        
        assert result.MessageResponseCode == result.ResponseCodes.OK, 'Push to datastore failed!'
        
        repo = self.workbench.get_repository(content)
        
        head = yield repo.checkout('master')
        
        yield self.reply(msg, content=head)
        

    @defer.inlineCallbacks
    def op_create_dataset_topics(self, content, headers, msg):
        """
        Creates ingestion and notification topics that can be used to publish ingestion
        data and notifications about ingestion.
        """

        # @TODO: adapted from temp reg publisher code in publisher_subscriber, update as appropriate
        msg = yield self.mc.create_instance(XS_TYPE)

        msg.exchange_space_name = 'swapmeet'

        rc = yield self._pscclient.declare_exchange_space(msg)
        #self._xs_id = rc.id_list[0]

        msg = yield self.mc.create_instance(XP_TYPE)
        msg.exchange_point_name = 'science_data'
        msg.exchange_space_id = self._xs_id

        rc = yield self._pscclient.declare_exchange_point(msg)
        #self._xp_id = rc.id_list[0]

        msg = yield self.mc.create_instance(TOPIC_TYPE)
        msg.topic_name = content.dataset_id
        msg.exchange_space_id = self._xs_id
        msg.exchange_point_id = self._xp_id

        rc = yield self._pscclient.declare_topic(msg)

        yield self.reply_ok(msg)

    class IngestSubscriber(Subscriber):
        """
        Specially derived Subscriber that routes received messages into the ingest service's
        standard receive method, as if it is one of the process receivers.
        """
        @defer.inlineCallbacks
        def _receive_handler(self, content, msg):
            yield self._process.receive(content, msg)

    def _ingest_data_topic_valid(self, ingest_data_topic):
        """
        Determines if the ingestion data topic is a valid topic for ingestion.
        The topic should have been registered via op_create_dataset_topics prior to
        ingestion.
        @TODO: this
        """
        log.debug("TODO: _ingest_data_topic_valid")
        return True

    @defer.inlineCallbacks
    def op_perform_ingest(self, content, headers, msg):
        """
        Start the ingestion process by setting up neccessary
        @TODO NO MORE MAGNET.TOPIC
        """
        log.info('<<<---@@@ Incoming perform_ingest request with "Perform Ingest" message')
        log.debug("...Content:\t" + str(content))

        # TODO: replace this from the msg itself with just dataset id
        ingest_data_topic = content.dataset_id

        # TODO: validate ingest_data_topic
        valid = self._ingest_data_topic_valid(ingest_data_topic)
        if not valid:
            log.error("Invalid data ingestion topic (%s), allowing it for now TODO" % ingest_data_topic)

        log.info('Setting up ingest topic for communication with a Dataset Agent: "%s"' % ingest_data_topic)
        self._subscriber = self.IngestSubscriber(xp_name="magnet.topic",
                                                 binding_key=ingest_data_topic,
                                                 process=self)
        yield self.register_life_cycle_object(self._subscriber) # move subscriber to active state

        def _timeout():
            # trigger execution to continue below with a False result
            log.info("Timed out in op_perform_ingest")
            self._defer_ingest.callback(False)

        log.info('Setting up ingest timeout with value: %i' % content.ingest_service_timeout)
        timeoutcb = reactor.callLater(content.ingest_service_timeout, _timeout)

        log.info('Notifying caller that ingest is ready by invoking op_ingest_ready() using routing key: "%s"' % content.reply_to)
        irmsg = yield self.mc.create_instance(INGESTION_READY_TYPE)
        irmsg.xp_name = "magnet.topic"
        irmsg.publish_topic = ingest_data_topic

        self.send(content.reply_to, operation='ingest_ready', content=irmsg)

        log.info("Yielding in op_perform_ingest for receive loop to complete")
        ingest_res = yield self._defer_ingest    # wait for other commands to finish the actual ingestion

        # common cleanup

        # reset ingestion deferred so we can use it again
        self._defer_ingest = defer.Deferred()

        # remove subscriber, deactivate it
        self._registered_life_cycle_objects.remove(self._subscriber)
        yield self._subscriber.terminate()
        self._subscriber = None

        if ingest_res:
            log.debug("Ingest succeeded, respond to original request")

            # we succeeded, cancel the timeout
            timeoutcb.cancel()

            # send notification we performed an ingest
            # @todo: This causes an AssertionError, please fix
            #yield self._notify_ingest(content)

            # now reply ok to the original message
            yield self.reply_ok(msg, content={})
        else:
            log.debug("Ingest failed, error back to original request")
            raise IngestionError("Ingestion failed", content.ResponseCodes.INTERNAL_SERVER_ERROR)

    @defer.inlineCallbacks
    def _notify_ingest(self, content):
        """
        Generate a notification/event that an ingest succeeded.
        @TODO: this is temporary, to be replaced
        """
        pub = Publisher(xp_name="event.topic", routing_key="_not.ingestion.ingest", process=self)
        yield pub.initialize()
        yield pub.activate()
        yield pub.publish(True)

    @defer.inlineCallbacks
    def op_recv_dataset(self, content, headers, msg):
        log.info("op_recv_dataset(%s)" % type(content))
        # this is NOT rpc
        yield msg.ack()

    @defer.inlineCallbacks
    def op_recv_chunk(self, content, headers, msg):
        log.info("op_recv_chunk(%s)" % type(content))
        # this is NOT rpc
        yield msg.ack()

    @defer.inlineCallbacks
    def op_recv_done(self, content, headers, msg):
        log.info("op_recv_done(%s)" % type(content))
        # this is NOT rpc
        yield msg.ack()

        # trigger the op_perform_ingest to complete!
        self._defer_ingest.callback(True)

class IngestionClient(ServiceClient):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        # Step 1: Delegate initialization to parent "ServiceClient"
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "ingestion"
        ServiceClient.__init__(self, proc, **kwargs)
        
        # Step 2: Perform Initialization
        self.mc = MessageClient(proc=self.proc)
#        self.rc = ResourceClient(proc=self.proc)

    @defer.inlineCallbacks
    def ingest(self):
        """
        No argument needed - just send a simple object....
        """
        yield self._check_init()
        
        repo, ab = self.proc.workbench.init_repository(addresslink_type)
        
        ab.person.add()

        p = repo.create_object(person_type)
        p.name = 'david'
        p.id = 59
        p.email = 'stringgggg'
        ab.person[0] = p
        
        #print 'AdressBook!',ab
        
        (content, headers, msg) = yield self.rpc_send('ingest', ab)
        
        defer.returnValue(content)
        
        

    @defer.inlineCallbacks
    def retrieve(self,dataset_id):
        """
        @brief Client method to Register a Resource Instance
        This method is used to generate a new resource instance of type
        Resource Type
        @param resource_type
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('retrieve', dataset_id)
        
        
        log.info('EOI Ingestion Service; Retrieve replied: '+str(content))
        # Return value should be a resource identity
        defer.returnValue(content)
        
        
        
    @defer.inlineCallbacks
    def perform_ingest(self, dataset_id, reply_to, ingest_service_timeout):
        """
        Start the ingest process by passing the Service a topic to communicate on, a
        routing key for intermediate replies (signaling that the ingest is ready), and
        a custom timeout for the ingest service (since it may take much longer than the
        default timeout to complete an ingest)
        """
        log.debug('-[]- Entered IngestionClient.perform_ingest()')
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()
        
        # Create the PerformIngestMessage
        begin_msg = yield self.mc.create_instance(PERFORM_INGEST_TYPE)
        begin_msg.dataset_id                = dataset_id
        begin_msg.reply_to                  = reply_to
        begin_msg.ingest_service_timeout    = ingest_service_timeout

        # Invoke [op_]() on the target service 'dispatcher_svc' via RPC
        log.info("@@@--->>> Sending 'perform_ingest' RPC message to ingestion service")
        content = ""
        (content, headers, msg) = yield self.rpc_send('perform_ingest', begin_msg, timeout=ingest_service_timeout+30)
        

        defer.returnValue(content)

    @defer.inlineCallbacks
    def create_dataset_topics(self, msg):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_dataset_topics', msg)
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def demo(self, ds_ingest_topic):
        """
        This is a temporary method used for testing.
        """
        yield self.proc.send(ds_ingest_topic, operation='recv_shell', content='demo_start')

        yield self.proc.send(ds_ingest_topic, operation='recv_chunk', content='demo_data1')
        yield self.proc.send(ds_ingest_topic, operation='recv_chunk', content='demo_data1')

        yield self.proc.send(ds_ingest_topic, operation='recv_done', content='demo_done')

# Spawn of the process using the module name
factory = ProcessFactory(IngestionService)



'''

#----------------------------#
# Application Startup
#----------------------------#
:: bash ::
bin/twistd -n cc -h amoeba.ucsd.edu -a sysname=eoitest res/apps/resource.app


#----------------------------#
# Begin_Ingest Testing
#----------------------------#
from ion.services.dm.ingestion.ingestion import IngestionClient
client = IngestionClient()
spawn('ingestion')

client.begin_ingest('ingest.topic.123iu2yr82', 'ready_routing_key', 1234)

'''
