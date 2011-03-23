#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry/resource_registry.py
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


from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.core.object import object_utils

person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)

BEGIN_INGEST_TYPE = object_utils.create_type_identifier(object_id=2002, version=1)

class EOIIngestionError(ApplicationError):
    """
    An error occured during the begin_ingest op of EOIIngestionService.
    """
    pass

class EOIIngestionService(ServiceProcess):
    """
    Place holder to move data between EOI and the datastore
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='eoi_ingest', version='0.1.0', dependencies=[])

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

        log.info('EOIIngestionService.__init__()')

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
        
        
    class IngestSubscriber(Subscriber):
        """
        Specially derived Subscriber that routes received messages into the ingest service's
        standard receive method, as if it is one of the process receivers.
        """
        @defer.inlineCallbacks
        def _receive_handler(self, content, msg):
            yield self._process.receive(content, msg)

    @defer.inlineCallbacks
    def op_perform_ingest(self, content, headers, msg):
        """
        Start the ingestion process by setting up neccessary
        """
        log.info('<<<---@@@ Incoming begin_ingest request with "Begin Ingest" message')
        log.debug("...Content:\t" + str(content))


        log.info('Setting up ingest topic for communication with a Dataset Agent: "%s"' % content.ds_ingest_topic)
        self._subscriber = self.IngestSubscriber(xp_name="magnet.topic",
                                                 binding_key=content.ds_ingest_topic,
                                                 process=self)
        yield self.register_life_cycle_object(self._subscriber) # move subscriber to active state

        def _timeout():
            # trigger execution to continue below with a False result
            log.info("Timed out in op_perform_ingest")
            self._defer_ingest.callback(False)

        log.info('Setting up ingest timeout with value: %i' % content.ingest_service_timeout)
        timeoutcb = reactor.callLater(content.ingest_service_timeout, _timeout)

        log.info('Notifying caller that ingest is ready by invoking RPC op_ingest_ready() using routing key: "%s"' % content.ready_routing_key)
        self.send(content.ready_routing_key, operation='ingest_ready', content=True)
        #yield self.rpc_send(content.ready_routing_key, operation='ingest_ready', content=True)

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
            yield self._notify_ingest(content)

            # now reply ok to the original message
            yield self.reply_ok(msg, content={'topic':content.ds_ingest_topic})
        else:
            log.debug("Ingest failed, error back to original request")
            raise EOIIngestionError("Ingestion failed", content.ResponseCodes.INTERNAL_SERVER_ERROR)
            #yield self.reply_err(msg, content="dyde")

    @defer.inlineCallbacks
    def _notify_ingest(self, content):
        """
        Generate a notification/event that an ingest succeeded.
        @TODO: this is temporary, to be replaced
        """
        pub = Publisher(xp_name="event.topic", routing_key="_not.eoi_ingest.ingest")
        yield pub.initialize()
        yield pub.activate()
        yield pub.publish(True)

    @defer.inlineCallbacks
    def op_recv_shell(self, content, headers, msg):
        log.info("op_recv_shell")
        # this is NOT rpc
        yield msg.ack()

    @defer.inlineCallbacks
    def op_recv_chunk(self, content, headers, msg):
        log.info("op_recv_chunk")
        # this is NOT rpc
        yield msg.ack()

    @defer.inlineCallbacks
    def op_recv_done(self, content, headers, msg):
        log.info("op_recv_done")
        # this is NOT rpc
        yield msg.ack()

        # trigger the op_perform_ingest to complete!
        self._defer_ingest.callback(True)

class EOIIngestionClient(ServiceClient):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        # Step 1: Delegate initialization to parent "ServiceClient"
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "eoi_ingest"
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
    def perform_ingest(self, ds_ingest_topic, ready_routing_key, ingest_service_timeout):
        """
        Start the ingest process by passing the Service a topic to communicate on, a
        routing key for intermediate replies (signaling that the ingest is ready), and
        a custom timeout for the ingest service (since it may take much longer than the
        default timeout to complete an ingest)
        """
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()
        
        # Create the BeginIngestMessage
        begin_msg = yield self.mc.create_instance(BEGIN_INGEST_TYPE)
        begin_msg.ds_ingest_topic        = ds_ingest_topic
        begin_msg.ready_routing_key       = ready_routing_key
        begin_msg.ingest_service_timeout = ingest_service_timeout

        # Invoke [op_]() on the target service 'dispatcher_svc' via RPC
        log.info("@@@--->>> Sending 'perform_ingest' RPC message to eoi_ingest service")
        content = ""
        (content, headers, msg) = yield self.rpc_send('perform_ingest', begin_msg, timeout=ingest_service_timeout+30)
        

        defer.returnValue(content)
        
        
    @defer.inlineCallbacks
    def demo(self, ds_ingest_topic):
        yield self.proc.send(ds_ingest_topic, operation='recv_shell', content='demo_start')

        yield self.proc.send(ds_ingest_topic, operation='recv_chunk', content='demo_data1')
        yield self.proc.send(ds_ingest_topic, operation='recv_chunk', content='demo_data1')

        yield self.proc.send(ds_ingest_topic, operation='recv_done', content='demo_done')

# Spawn of the process using the module name
factory = ProcessFactory(EOIIngestionService)



'''

#----------------------------#
# Application Startup
#----------------------------#
:: bash ::
bin/twistd -n cc -h amoeba.ucsd.edu -a sysname=eoitest res/apps/resource.app


#----------------------------#
# Begin_Ingest Testing
#----------------------------#
from ion.services.dm.ingestion.eoi_ingester import EOIIngestionClient
client = EOIIngestionClient()
spawn('eoi_ingest')

client.begin_ingest('ingest.topic.123iu2yr82', 'ready_routing_key', 1234)

'''
