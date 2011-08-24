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

import time, calendar
from ion.services.dm.distribution.events import DatasetSupplementAddedEventPublisher, DatasourceUnavailableEventPublisher, DatasetChangeEventPublisher, IngestionProcessingEventPublisher, get_events_exchange_point, DatasetStreamingEventSubscriber
import ion.util.ionlog
from twisted.internet import defer, reactor
from twisted.python import reflect
from ion.core.object.workbench import BLOBS_REQUSET_MESSAGE_TYPE
import base64
import pprint

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError
from ion.services.dm.distribution.publisher_subscriber import Subscriber, PublisherFactory

from ion.core.object.cdm_methods import attribute_merge, variables

from ion.core.exception import ApplicationError

# For testing - used in the client
from ion.services.dm.distribution.pubsub_service import PubSubClient, XS_TYPE, XP_TYPE, TOPIC_TYPE
from ion.services.coi import datastore

from ion.core.exception import ReceivedApplicationError, ReceivedError, ReceivedContainerError

from ion.core.object.gpb_wrapper import OOIObjectError

from ion.core import ioninit
from ion.core.object import object_utils, gpb_wrapper

import logging
CONF = ioninit.config(__name__)
log = ion.util.ionlog.getLogger(__name__)


CDM_DATASET_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)

CDM_SINT_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10009, version=1)
CDM_UINT_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10010, version=1)
CDM_LSINT_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10011, version=1)
CDM_LUINT_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10012, version=1)

CDM_FLOAT_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10013, version=1)
CDM_DOUBLE_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10014, version=1)

CDM_STRING_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10015, version=1)
CDM_OPAQUE_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10016, version=1)

CDM_BOUNDED_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10021, version=1)
CDM_ARRAY_STRUCTURE_TYPE = object_utils.create_type_identifier(object_id=10025, version=1)

SUPPLEMENT_MSG_TYPE = object_utils.create_type_identifier(object_id=2001, version=1)
PERFORM_INGEST_MSG_TYPE = object_utils.create_type_identifier(object_id=2002, version=1)
CREATE_DATASET_TOPICS_MSG_TYPE = object_utils.create_type_identifier(object_id=2003, version=1)
INGESTION_READY_TYPE = object_utils.create_type_identifier(object_id=2004, version=1)
DAQ_COMPLETE_MSG_TYPE = object_utils.create_type_identifier(object_id=2005, version=1)

BLOBS_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=52, version=1)


class IngestionError(ApplicationError):
    """
    An error occured during the begin_ingest op of IngestionService.
    """
    pass


# Declare a few strings:
OK              = 'OK'
UNAVAILABLE     = 'EXTERNAL_SERVER_UNAVAILABLE'
SERVER_ERROR    = 'EXTERNAL_SERVER_ERROR'
NO_NEW_DATA     = 'NO_NEW_DATA'
AGENT_ERROR     = 'AGENT_ERROR'


# Supplememt added event message dict:

EM_DATA_SOURCE  = 'datasource_id'
EM_DATASET      = 'dataset_id'
EM_TITLE        = 'title'
EM_URL          = 'url'
EM_START_DATE   = 'start_datetime_millis'
EM_END_DATE     = 'end_datetime_millis'
EM_TIMESTEPS    = 'number_of_timesteps'

EM_ERROR        = 'error_explanation'




class IngestionService(ServiceProcess):
    """
    DM R1 Ingestion service.
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='ingestion', version='0.1.0', dependencies=[])

    # Declare the excluded types for repository operations
    excluded_data_array_types = (CDM_SINT_ARRAY_TYPE, CDM_UINT_ARRAY_TYPE, CDM_LSINT_ARRAY_TYPE, CDM_LUINT_ARRAY_TYPE,
                                 CDM_DOUBLE_ARRAY_TYPE, CDM_FLOAT_ARRAY_TYPE, CDM_STRING_ARRAY_TYPE,
                                 CDM_OPAQUE_ARRAY_TYPE)





    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.

        ServiceProcess.__init__(self, *args, **kwargs)


        self.op_fetch_blobs = self.workbench.op_fetch_blobs

        self._defer_ingest = defer.Deferred()       # waited on by op_ingest to signal end of ingestion

        self.rc = ResourceClient(proc=self)
        self.mc = MessageClient(proc=self)

        self._pscclient = PubSubClient(proc=self)

        self.dsc = datastore.DataStoreClient(proc=self)

        self.dataset = None
        self.data_source = None

        self._ingestion_terminating = False

        self._ingestion_processing_publisher = IngestionProcessingEventPublisher(process=self)
        self.add_life_cycle_object(self._ingestion_processing_publisher)        # will move through lifecycle states as appropriate

        log.info('IngestionService.__init__()')

    @defer.inlineCallbacks
    def slc_activate(self):

        log.info('Activation - Start')

        pub_factory = PublisherFactory(process=self)

        self._notify_ingest_publisher = yield pub_factory.build(publisher_type=DatasetSupplementAddedEventPublisher)

        self._notify_dataset_change_publisher = yield pub_factory.build(publisher_type=DatasetChangeEventPublisher)

        self._notify_unavailable_publisher = yield pub_factory.build(publisher_type=DatasourceUnavailableEventPublisher)

        log.info('Activation - Complete')


    @defer.inlineCallbacks
    def op_create_dataset_topics(self, content, headers, msg_in):
        """
        Creates ingestion and notification topics that can be used to publish ingestion
        data and notifications about ingestion.
        """

        log.info('op_create_dataset_topics - Start')

        # OOIION-4: made creation of topics via PSC configurable (def: false) due to performance

        if CONF.getValue('create_psc_dataset_topics', False):
            # @TODO: adapted from temp reg publisher code in publisher_subscriber, update as appropriate
            msg = yield self.mc.create_instance(XS_TYPE)

            msg.exchange_space_name = 'swapmeet'

            rc = yield self._pscclient.declare_exchange_space(msg)
            self._xs_id = rc.id_list[0]

            msg = yield self.mc.create_instance(XP_TYPE)
            msg.exchange_point_name = 'science_data'
            msg.exchange_space_id = self._xs_id

            rc = yield self._pscclient.declare_exchange_point(msg)
            self._xp_id = rc.id_list[0]

            msg = yield self.mc.create_instance(TOPIC_TYPE)
            msg.topic_name = content.dataset_id
            msg.exchange_space_id = self._xs_id
            msg.exchange_point_id = self._xp_id

            rc = yield self._pscclient.declare_topic(msg)
        else:
            log.info("create_psc_dataset_topics CONF option not set or False, no-op'ing this method")

        yield self.reply_ok(msg_in)

        log.info('op_create_dataset_topics - Complete')

    class IngestSubscriber(DatasetStreamingEventSubscriber):
        """
        Specially derived EventSubscriber that routes received messages into a custom handler that is similar to
        the main Process.receive method, but eliminates problems and handles Exceptions better.
        """
        @defer.inlineCallbacks
        def _receive_handler(self, content, msg):
            """
            Let the ondata method handle acking the message.
            """
            yield self.ondata(content, msg)

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
    def _prepare_ingest(self, content):
        """
        Factor out the preparation for ingestion so that we can unit test functionality
        """

        log.debug('_prepare_ingest - Start')

        # Get the current state of the dataset:
        try:
            self.dataset = yield self.rc.get_instance(content.dataset_id, excluded_types=[CDM_BOUNDED_ARRAY_TYPE])

        except ResourceClientError, rce:
           log.exception('Could not get dataset resource!')
           raise IngestionError('Could not get the dataset resource from the datastore')

        log.info('Got dataset resource')

        try:
            self.data_source = yield self.rc.get_instance(content.datasource_id)
        except ResourceClientError, rce:
           log.exception('Could not get datasource resource!')
           raise IngestionError('Could not get the datasource resource from the datastore.  Cause: %s' % str(rce))

        log.info('Got datasource resource')


        # Get the bounded arrays but not the ndarrays
        ba_links = []
        for var in self.dataset.root_group.variables:
            var_links = var.content.bounded_arrays.GetLinks()
            ba_links.extend(var_links)

        yield self.dataset.Repository.fetch_links(ba_links)

        log.debug('_prepare_ingest - Complete')

        defer.returnValue(None)

    @defer.inlineCallbacks
    def _setup_ingestion_topic(self, content, convid):

        log.debug('_setup_ingestion_topic - Start')

        # TODO: replace this from the msg itself with just dataset id
        ingest_data_topic = content.dataset_id

        # TODO: validate ingest_data_topic
        valid = self._ingest_data_topic_valid(ingest_data_topic)
        if not valid:
            log.error("Invalid data ingestion topic (%s), allowing it for now TODO" % ingest_data_topic)

        log.info('Setting up ingest topic for communication with a Dataset Agent: "%s"' % ingest_data_topic)
        self._subscriber = self.IngestSubscriber(origin=ingest_data_topic, process=self)
        self._subscriber.ondata = lambda payload, msg: self._handle_ingestion_msg(payload, msg, convid)

        yield self.register_life_cycle_object(self._subscriber) # move subscriber to active state

        log.debug('_setup_ingestion_topic - Complete')

        defer.returnValue(self._subscriber._binding_key)

    @defer.inlineCallbacks
    def _handle_ingestion_msg(self, payload, msg, convid):
        """
        Handles recv_dataset, recv_chunk, recv_done

        This code is basically Process.receive, but without the conversation/user-id business which comprises most
        of that. It also adds proper error handling in the context of the ingestion, so if one of these messages
        triggers one, it will error out of the op_ingest call appropriately.
        """

        if self._ingestion_terminating:
            log.debug("Attempting to route received data message while ingestion subscriber terminating, discarding the message!")
            # set msg._state to anything to prevent auto-acking
            msg._state = "ACKED"
            defer.returnValue(False)

        try:
            opname = payload.get('op', '')
            content = payload.get('content', '')    # should be None, but this is how Process' receive does it

            if opname == 'recv_dataset':
                yield self._ingest_op_recv_dataset(content, payload, msg, convid)
            elif opname == 'recv_chunk':
                yield self._ingest_op_recv_chunk(content, payload, msg, convid)
            elif opname == 'recv_done':
                yield self._ingest_op_recv_done(content, payload, msg, convid)
            else:
                raise IngestionError('Unknown operation specified')

        except Exception, ex:

            # set flag to prevent routing while in process of termination
            self._ingestion_terminating = True

            # ack the message to make the receiver stack happy (subscriber still active here, so this is ok)
            yield msg.ack()

            # all error handling goes back to op_ingest
            self._defer_ingest.errback(ex)

    @defer.inlineCallbacks
    def op_ingest(self, content, headers, msg):
        """
        Start the ingestion process by setting up necessary
        @TODO NO MORE MAGNET.TOPIC
        """
        log.info('op_ingest - Start')

        if content.MessageType != PERFORM_INGEST_MSG_TYPE:
            raise IngestionError('Expected message type PerformIngestRequest, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)

        # Keep track of the update branch name
        self.update_branch_key = None

        yield self._prepare_ingest(content)

        log.info('Created dataset details, Now setup subscriber...')

        ingest_data_topic = yield self._setup_ingestion_topic(content, headers.get('conv-id', "no-conv-id"))


        def _timeout():
            log.info("Timed out in op_perform_ingest")
            self._defer_ingest.errback(IngestionError('Time out in communication between the JAW and the Ingestion service', content.ResponseCodes.TIMEOUT))

        log.info('Setting up ingest timeout with value: %i' % content.ingest_service_timeout)
        self.timeoutcb = reactor.callLater(content.ingest_service_timeout, _timeout)
        self.timeoutcb.ingest_service_timeout = content.ingest_service_timeout

        log.info(
            'Notifying caller that ingest is ready by invoking op_ingest_ready() using routing key: "%s"' % content.reply_to)
        irmsg = yield self.mc.create_instance(INGESTION_READY_TYPE)
        irmsg.xp_name = get_events_exchange_point()
        irmsg.publish_topic = ingest_data_topic

        self.send(content.reply_to, operation='ingest_ready', content=irmsg)

        log.info("Yielding in op_perform_ingest for receive loop to complete")

        # exceptions like IngestionError/ApplicationError can be raised while waiting for this deferred - they will
        # get pushed via errback to here. We mostly just want them to go through the usual exception stack, but
        # we should send out a failure notification before we do so.
        try:
            ingest_res = yield self._defer_ingest    # wait for other commands to finish the actual ingestion
        except Exception, ex:

            # clear the repository
            self.workbench.clear_repository_key(content.dataset_id)

            # we have to notify that there is a failure, so get details and setup the dict to pass to notify_ingest.
            data_details = self.get_data_details(content)
            ingest_res={EM_ERROR:'Ingestion Failed: %s' % str(ex.message)}
            ingest_res.update(data_details)

            log.exception("Error occured while waiting for ingestion to complete:")

            yield self._notify_ingest(ingest_res)

            # reraise - in the case of ApplicationError, will simply reply to the original sender
            # do NOT reraise in the case of a timeout on our side - JAW will timeout client-side
            if hasattr(ex, 'response_code') and ex.response_code == content.ResponseCodes.TIMEOUT:
                # ack the msg
                #yield msg.ack()

                # just return from here
                defer.returnValue(False)
            else:
                raise ex

        finally:
            # we finished waiting (either success/failure/timeout), cancel the timeout if active
            if self.timeoutcb.active():
                self.timeoutcb.cancel()

            # reset ingestion deferred so we can use it again
            self._defer_ingest = defer.Deferred()

            # remove subscriber, deactivate it
            self._registered_life_cycle_objects.remove(self._subscriber)
            yield self._subscriber.terminate()
            self._subscriber = None

            # reset terminating flag after shutting down Subscriber
            # WARNING: MESSAGES MAY STILL ROUTE, CHECK STATE OF TIMEOUTCB IN EACH HANDLER
            self._ingestion_terminating = False

        data_details = self.get_data_details(content)

        if isinstance(ingest_res, dict):
            ingest_res.update(data_details)
        else:
            ingest_res={EM_ERROR:'Ingestion Failed!'}
            ingest_res.update(data_details)


        resources = []

        if ingest_res.has_key(EM_ERROR):
            log.info("Ingest Failed! %s" % str(ingest_res))

            # Make sure we have the master branch checked out:
            if self.dataset.Repository.current_branch_key != self.dataset.Repository.branchnicknames.get('master'):
                self.dataset.Repository.reset()
                yield self.dataset.Repository.checkout(branchname='master')

            # Make sure we don't push a dataset that is partly merged or ingested
            if len(self.dataset.Repository.branches) is not 1:

                if self.update_branch_key is None:
                    log.warn(str(self.dataset))
                    raise IngestionError('Update branch key is not set but the dataset has more than one branch',500)

                # Remove the branch that was created
                self.dataset.Repository.remove_branch(self.update_branch_key)

            # Set the LCS to inactive if there was an error during Merge
            #self.dataset.ResourceLifeCycleState = self.dataset.INACTIVE
            #@TODO This should only happen if there was an error during merge - since the state is now reset - we don't need it?


        else:
            log.info("Ingest succeeded!")

            resources.append(self.dataset)

            # If the dataset / source is new 
            if self.dataset.ResourceLifeCycleState != self.dataset.ACTIVE:

                self.dataset.ResourceLifeCycleState = self.dataset.ACTIVE


        # Do not push the dataset if it has more than one branch
        if len(self.dataset.Repository.branches) is not 1:
            log.warn(str(self.dataset))
            # raise an exception and kick over the service so the resource is reset
            raise IngestionError('Dataset was left in an invalid state with more than one branch.',500)


        try:
            yield self.rc.put_instance(self.dataset)
        except ResourceClientError, rce:
            ingest_res[EM_ERROR] = 'Ingestion put_instance operation failed!'
            log.exception('Ingestion put_instance operation failed!')


        yield self._notify_ingest(ingest_res)

        self.dataset=None
        self.data_source = None

        # now reply ok to the original message
        yield self.reply_ok(msg)



        log.info('op_ingest - Complete')


    def get_data_details(self, content):
        try:
            att = self.dataset.root_group.FindAttributeByName('title')
            title = att.GetValue()
        except OOIObjectError, oe:
            log.warn('No title attribute found in Dataset: "%s"' % content.dataset_id)
            title = 'None Given'


        try:
            att = self.dataset.root_group.FindAttributeByName('references')
            references = att.GetValue()
        except OOIObjectError, oe:
            log.warn('No title attribute found in Dataset: "%s"' % content.dataset_id)
            references = 'None Given'



        data_details = {EM_TITLE:title,
                       EM_URL:references,
                       EM_DATA_SOURCE:content.datasource_id,
                       EM_DATASET:content.dataset_id,
                       }

        return data_details


    @defer.inlineCallbacks
    def _notify_ingest(self, ingest_res):
        """
        Generate a notification/event that an ingest succeeded.

        This method is really not needed but I like it for testing...
        """

        log.debug('_notify_ingest - Start')


        if ingest_res.has_key(EM_ERROR):
            # Report an error with the data source
            datasource_id = ingest_res[EM_DATA_SOURCE]

            # Don't use **kw args - it may fail depending on what is in the dict...
            #yield self._notify_unavailable_publisher.create_and_publish_event(origin=datasource_id, **ingest_res)

            msg = yield self._notify_unavailable_publisher.create_event(origin=datasource_id)
            self._notify_unavailable_publisher._set_msg_fields(msg.additional_data, ingest_res.copy())
            yield self._notify_unavailable_publisher.publish_event(msg,origin=datasource_id)

        else:
            # Report a successful update to the dataset
            dataset_id = ingest_res[EM_DATASET]

            # Don't use **kw args - it may fail depending on what is in the dict...
            #yield self._notify_ingest_publisher.create_and_publish_event(origin=dataset_id, **ingest_res)

            msg = yield self._notify_ingest_publisher.create_event(origin=dataset_id)
            self._notify_ingest_publisher._set_msg_fields(msg.additional_data, ingest_res.copy())
            yield self._notify_ingest_publisher.publish_event(msg, origin=dataset_id)

            yield self._notify_dataset_change_publisher.create_and_publish_event(origin=dataset_id, dataset_id=dataset_id)


        log.debug('_notify_ingest - Complete')


    @defer.inlineCallbacks
    def _ingest_op_recv_dataset(self, content, headers, msg, convid="unknown"):

        log.info('_ingest_op_recv_dataset - Start')

        if self._ingestion_terminating or not self.timeoutcb.active():
            log.debug("_ingest_op_recv_dataset received a routed message AFTER shutdown occured (probably during an ApplicationError). Discarding this message!")
            # set msg._state to anything to prevent auto-ack
            msg._state = "ACKED"
            defer.returnValue(None)

        # reset timeout
        cbtimeout = self.timeoutcb.ingest_service_timeout
        cbfunc = self.timeoutcb.func

        log.info('Setting timeout to %d seconds from now' % cbtimeout)
        self.timeoutcb.cancel()
        self.timeoutcb = reactor.callLater(cbtimeout, cbfunc)
        self.timeoutcb.ingest_service_timeout = cbtimeout

        # notify JAW and others via event that we are still processing
        yield self._ingestion_processing_publisher.create_and_publish_event(origin=self.dataset.ResourceIdentity,
                                                                            dataset_id=self.dataset.ResourceIdentity,
                                                                            ingestion_process_id=self.id.full,
                                                                            conv_id=convid,
                                                                            processing_step="dataset")

        if content.MessageType != CDM_DATASET_TYPE:
            raise IngestionError('Expected message type CDM Dataset Type, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)

        if self.dataset is None:
            raise IngestionError('Calling recv_dataset in an invalid state. No Dataset checked out to ingest.')

        if self.dataset.Repository.status is not self.dataset.Repository.UPTODATE:
            raise IngestionError('Calling recv_dataset in an invalid state. Dataset is already modified.')

        self.update_branch_key = self.dataset.CreateUpdateBranch(content.MessageObject)

        group = self.dataset.root_group

        # Clear any bounded arrays which are empty. Create content field if it is not present
        for var in group.variables:
            if var.IsFieldSet('content'):
                content = var.content

                if len(content.bounded_arrays) > 0:
                    i = 0
                    while i < len(content.bounded_arrays):
                        ba = content.bounded_arrays[i]

                        # Clear empty bounded arrays that may be sent by dac
                        if not ba.IsFieldSet('ndarray'):
                            del content.bounded_arrays[i]

                            continue
                        else:
                            i += 1
            else:
                var.content = self.dataset.CreateObject(CDM_ARRAY_STRUCTURE_TYPE)

        yield msg.ack()

        log.info('_ingest_op_recv_dataset - Complete')


    @defer.inlineCallbacks
    def _ingest_op_recv_chunk(self, content, headers, msg, convid="unknown"):

        log.info('_ingest_op_recv_chunk - Start')

        if self._ingestion_terminating or not self.timeoutcb.active():
            log.debug("_ingest_op_recv_chunk received a routed message AFTER shutdown occured (probably during an ApplicationError). Discarding this message!")
            # set msg._state to anything to prevent auto-ack
            msg._state = "ACKED"
            defer.returnValue(None)

        # reset timeout
        cbtimeout = self.timeoutcb.ingest_service_timeout
        cbfunc = self.timeoutcb.func

        log.info('Setting timeout to %d seconds from now' % cbtimeout)
        self.timeoutcb.cancel()
        self.timeoutcb = reactor.callLater(cbtimeout, cbfunc)
        self.timeoutcb.ingest_service_timeout = cbtimeout

        # this is NOT rpc
        if content.MessageType != SUPPLEMENT_MSG_TYPE:
            raise IngestionError('Expected message type SupplementMessageType, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)
            
        if self.dataset is None:
            raise IngestionError('Calling recv_chunk in an invalid state. No Dataset checked out to ingest.')

        if self.dataset.ResourceLifeCycleState is not self.dataset.UPDATE:
            raise IngestionError('Calling recv_chunk in an invalid state. Dataset is not on an update branch! (currently: %s)' % str(self.dataset.ResourceLifeCycleState))

        # OOIION-191: sanity check field dataset_id disabled as DatasetAgent does not have the information when making these messages.
        #if content.dataset_id != self.dataset.ResourceIdentity:
        #    raise IngestionError('Calling recv_chunk with a dataset that does not match the received chunk!.')

        # notify JAW and others via event that we are still processing
        yield self._ingestion_processing_publisher.create_and_publish_event(origin=self.dataset.ResourceIdentity,
                                                                            dataset_id=self.dataset.ResourceIdentity,
                                                                            ingestion_process_id=self.id.full,
                                                                            conv_id=convid,
                                                                            processing_step="chunk")

        # Get the group out of the datset
        group = self.dataset.root_group

        # get the bounded array out of the message
        ba = content.bounded_array


        # Create a blobs message to send to the datastore with the ndarray
        blobs_msg = yield self.mc.create_instance(BLOBS_MESSAGE_TYPE)
        ndarray_element = content.Repository.index_hash.get(ba.ndarray.MyId)
        obj = content.Repository._wrap_message_object(ndarray_element._element)

        link = blobs_msg.blob_elements.add()
        link.SetLink(obj)

        # Put it to the datastore
        try:
            yield self.dsc.put_blobs(blobs_msg)
        except ReceivedError, re:
            log.error(re)
            raise IngestionError('Could not put blob in received chunk to the datastore.')

        # Now add the bounded array, but not the ndarray to the dataset in the ingestion service
        log.debug('Adding content to variable name: %s' % content.variable_name)
        try:
            var = group.FindVariableByName(content.variable_name)
        except gpb_wrapper.OOIObjectError, oe:
            log.error(str(oe))
            raise IngestionError('Expected variable name %s not found in the dataset' % (content.variable_name))

        ba_link = var.content.bounded_arrays.add()
        my_ba = ba_link.Repository.copy_object(ba, deep_copy=False)
        ba_link.SetLink(my_ba)

        yield msg.ack()

        log.info('_ingest_op_recv_chunk - Complete')


    @defer.inlineCallbacks
    def _ingest_op_recv_done(self, content, headers, msg, convid="unknown"):
        """
        @TODO deal with FMRC datasets and supplements
        """

        log.info('_ingest_op_recv_done - Start')

        if self._ingestion_terminating or not self.timeoutcb.active():
            log.debug("_ingest_op_recv_done received a routed message AFTER shutdown occured (probably during an ApplicationError). Discarding this message!")
            # set msg._state to anything to prevent auto-ack
            msg._state = "ACKED"
            defer.returnValue(None)

        # notify JAW and others via event that we are still processing
        yield self._ingestion_processing_publisher.create_and_publish_event(origin=self.dataset.ResourceIdentity,
                                                                            dataset_id=self.dataset.ResourceIdentity,
                                                                            ingestion_process_id=self.id.full,
                                                                            conv_id=convid,
                                                                            processing_step="done")

        log.info('Cancelling timeout!')
        self.timeoutcb.cancel()
        

        log.debug(type(content))
        log.debug(str(content.Message))

        if content.MessageType != DAQ_COMPLETE_MSG_TYPE:
            raise IngestionError('Expected message type Data Acquasition Complete Message Type, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)



        if content.status != content.StatusCode.OK:

            result = {}
            
            st = content.status
            if st == 1:
                status = UNAVAILABLE
            elif st == 2:
                status = SERVER_ERROR
            elif st == 3:
                status = NO_NEW_DATA
            elif st == 4:
                status = AGENT_ERROR
            else:
                raise IngestionError('Unexpected StatusCode Enum value in Data Acquisition Complete Message')

            result[EM_ERROR] = '%s: %s' % (status, content.status_body)

        else:

            #@TODO ask dave for help here - how can I chain these callbacks?

            if self.data_source.aggregation_rule == self.data_source.AggregationRule.OVERLAP:

                result = yield self._merge_overlapping_supplement()

            elif self.data_source.aggregation_rule == self.data_source.AggregationRule.OVERWRITE:

                result = yield self._merge_overwrite_supplement()

            elif self.data_source.aggregation_rule == self.data_source.AggregationRule.FMRC:

                result = yield self._merge_fmrc_supplement()



        # this is NOT rpc
        yield msg.ack()


        # trigger the op_perform_ingest to complete!
        self._defer_ingest.callback(result)

        log.info('_ingest_op_recv_done - Complete')



    @defer.inlineCallbacks
    def _merge_overwrite_supplement(self):

        log.debug('_merge_overwrite_supplement - Start')
        is_overwrite = True

        # Perform necessary premerge operations and unpack the result
        cur_root, \
        cur_etime, \
        cur_agg_dim_length, \
        sup_root, \
        sup_stime, \
        sup_etime, \
        sup_agg_dim_length, \
        sup_agg_dim_name, \
        sup_fcst_dim_name, \
        is_new_ds, \
        result = yield self.__premerge(is_fmrc=False)


        # Calculate offsets and indices for positioning the supplement in the dataset
        log.debug('************* START  __calculate_merge_offsets() *************')
        
        sup_sindex, \
        sup_eindex, \
        insertion_offset, \
        runtime_offset, \
        forecast_offset = yield self.__calculate_merge_offsets(is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_stime, sup_etime, cur_etime, cur_agg_dim_length, sup_agg_dim_length, sup_fcst_dim_name)
        
        log.debug('************* END    __calculate_merge_offsets() *************')

        
        log.info('>> cur_agg_dim_length           = %i' % cur_agg_dim_length)
        log.info('>> sup_agg_dim_length           = %i' % sup_agg_dim_length)
        log.info('>> sup_sindex                   = %i' % sup_sindex)
        log.info('>> sup_eindex                   = %i' % sup_eindex)
        log.info('>> insertion_offset             = %i' % insertion_offset)
        log.info('>> (FMRC) runtime_offset hours  = %i' % runtime_offset)
        log.info('>> (FMRC) forecast_offset hours = %i' % forecast_offset)


        # Perform the merge
        merge_res = yield self.__merge(is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_fcst_dim_name, sup_stime, cur_etime, sup_sindex, sup_eindex, insertion_offset, runtime_offset, forecast_offset, is_new_ds)
        result.update(merge_res)


        log.debug('_merge_overwrite_supplement - Complete')
        defer.returnValue(result)


    @defer.inlineCallbacks
    def _merge_fmrc_supplement(self):

        log.debug('_merge_fmrc_supplement - Start')
        is_overwrite = False

        # Perform necessary premerge operations and unpack the result
        cur_root, \
        cur_etime, \
        cur_agg_dim_length, \
        sup_root, \
        sup_stime, \
        sup_etime, \
        sup_agg_dim_length, \
        sup_agg_dim_name, \
        sup_fcst_dim_name, \
        is_new_ds, \
        result = yield self.__premerge(is_fmrc=True)


        # Calculate offsets and indices for positioning the supplement in the dataset
        log.debug('************* START  __calculate_merge_offsets() *************')
        
        sup_sindex, \
        sup_eindex, \
        insertion_offset, \
        runtime_offset, \
        forecast_offset = yield self.__calculate_merge_offsets(is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_stime, sup_etime, cur_etime, cur_agg_dim_length, sup_agg_dim_length, sup_fcst_dim_name)
        
        log.debug('************* END    __calculate_merge_offsets() *************')

        
        log.info('>> cur_agg_dim_length           = %i' % cur_agg_dim_length)
        log.info('>> sup_agg_dim_length           = %i' % sup_agg_dim_length)
        log.info('>> sup_sindex                   = %i' % sup_sindex)
        log.info('>> sup_eindex                   = %i' % sup_eindex)
        log.info('>> insertion_offset             = %i' % insertion_offset)
        log.info('>> (FMRC) runtime_offset hours  = %i' % runtime_offset)
        log.info('>> (FMRC) forecast_offset hours = %i' % forecast_offset)


        # Perform the merge
        merge_res = yield self.__merge(is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_fcst_dim_name, sup_stime, cur_etime, sup_sindex, sup_eindex, insertion_offset, runtime_offset, forecast_offset, is_new_ds)
        result.update(merge_res)


        log.debug('_merge_overlapping_supplement - Complete')
        defer.returnValue(result)



    @defer.inlineCallbacks
    def _merge_overlapping_supplement(self):

        log.debug('_merge_overlapping_supplement - Start')
        is_overwrite = False

        # Perform necessary premerge operations and unpack the result
        cur_root, \
        cur_etime, \
        cur_agg_dim_length, \
        sup_root, \
        sup_stime, \
        sup_etime, \
        sup_agg_dim_length, \
        sup_agg_dim_name, \
        sup_fcst_dim_name, \
        is_new_ds, \
        result = yield self.__premerge(is_fmrc=False)


        # Calculate offsets and indices for positioning the supplement in the dataset
        log.debug('************* START  __calculate_merge_offsets() *************')
        
        sup_sindex, \
        sup_eindex, \
        insertion_offset, \
        runtime_offset, \
        forecast_offset = yield self.__calculate_merge_offsets(is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_stime, sup_etime, cur_etime, cur_agg_dim_length, sup_agg_dim_length, sup_fcst_dim_name)
        
        log.debug('************* END    __calculate_merge_offsets() *************')

        
        log.info('>> cur_agg_dim_length           = %i' % cur_agg_dim_length)
        log.info('>> sup_agg_dim_length           = %i' % sup_agg_dim_length)
        log.info('>> sup_sindex                   = %i' % sup_sindex)
        log.info('>> sup_eindex                   = %i' % sup_eindex)
        log.info('>> insertion_offset             = %i' % insertion_offset)
        log.info('>> (FMRC) runtime_offset hours  = %i' % runtime_offset)
        log.info('>> (FMRC) forecast_offset hours = %i' % forecast_offset)


        # Perform the merge
        merge_res = yield self.__merge(is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_fcst_dim_name, sup_stime, cur_etime, sup_sindex, sup_eindex, insertion_offset, runtime_offset, forecast_offset, is_new_ds)
        result.update(merge_res)


        log.debug('_merge_overlapping_supplement - Complete')
        defer.returnValue(result)
        

    @defer.inlineCallbacks
    def __premerge(self, is_fmrc=False):
        """
        Performs the following steps to facilitate common merge operations:
        1) Provides basic sanity checks on the dataset
        2) Ensures merge data is commited in the datasets repository
        3) Extracts time aggregation dimension and other fields to
             be used in subsequent merge operations (such as in
             _merge_overwrite_supplement() and _merge_overlapping_supplement()
             
        @return: A tuple containing the following items (in order):
                 sup_stime - the start time of the merge dataset in seconds
                 sup_etime - the end time of the merge dataset in seconds
        """
        
        # A little sanity check on entering recv_done...
        if len(self.dataset.Repository.branches) != 2:
            raise IngestionError('The dataset is in a bad state - there should be two branches (currently %d) in the repository state on entering recv_done.' % len(self.dataset.Repository.branches), 500)


        # Commit the current state of the supplement - ingest of new content is complete
        self.dataset.Repository.commit('Ingest received complete notification.')

        # The current branch on entering recv done is the supplement branch
        merge_branch = self.dataset.Repository.current_branch_key()

        # Merge it with the current state of the dataset in the datastore
        yield self.dataset.MergeWith(branchname=merge_branch, parent_branch='master')

        #Remove the head for the supplement - there is only one current state once the merge is complete!
        self.dataset.Repository.remove_branch(merge_branch)


        # Get the cur_root group of the current state of the dataset
        cur_root = self.dataset.root_group

        # Get the cur_root group of the supplement we are merging
        sup_root = self.dataset.Merge[0].root_group

        # Determine which time variable to aggregate on
        sup_agg_dim       = None
        sup_agg_dim_name  = None
        sup_fcst_dim_name = None
        if is_fmrc:
            log.info('(FMRC) Determining model time and forecast time dimension names')
            sup_agg_dim_name  = 'run'
            sup_fcst_dim_name = 'time'
            try:
                sup_agg_dim = sup_root.FindDimensionByName(sup_agg_dim_name)
            except OOIObjectError, oe:
                raise IngestionError('FMRC Supplement is missing the dimension "%s"' % sup_agg_dim_name)
            
        else:
            log.info('Starting Find Dimension Loop')
    
            time_vars = self._find_time_var(sup_root)
    
            if len(time_vars) is 0:
                result={EM_ERROR:'Error during ingestion: No Time variable found!'}
                defer.returnValue(result)
    
            # Determine the inner most dimension on which we are aggregating
            dimension_order = []
            for merge_var in time_vars:
    
                # Add each dimension in reverse order so that the inside dimension is always in front... to determine the time aggregation dimension
                for merge_dim in reversed(merge_var.shape):
    
                    if merge_dim not in dimension_order:
                        dimension_order.insert(0, merge_dim)
    
            #print 'FINAL DIMENSION ORDER:'
            #print [ dim.name for dim in dimension_order]
    
            # This is the inner most!
            sup_agg_dim = dimension_order[0]
            sup_agg_dim_name = sup_agg_dim.name
        
        log.info('Merge aggregation dimension name is: %s' % sup_agg_dim_name)
        log.info('(FMRC) Forecast time dimension name is: %s' % sup_fcst_dim_name)


        # Retrieve various attributes about the time dimensions
        sup_agg_dim_length = sup_agg_dim.length
        result = {EM_TIMESTEPS:sup_agg_dim_length}

        cur_agg_dim_length = 0
        try:
            agg_dim = cur_root.FindDimensionByName(sup_agg_dim_name)
            cur_agg_dim_length = agg_dim.length
            log.info('Aggregation offset from current dataset: %d' % cur_agg_dim_length)

        except OOIObjectError, oe:
            log.debug("Time Dimension of data supplement is not present in current dataset.  Dimension name: '%s'.  Cause: %s" % (sup_agg_dim_name, str(oe)))


        # Get the start time of the supplement
        try:
            string_time = sup_root.FindAttributeByName('ion_time_coverage_start')
            sup_stime = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
            log.debug('Supplement Start Time: %s (%i)' % (string_time.GetValue(), sup_stime))

        except OOIObjectError, oe:
            raise IngestionError('No start time attribute found in dataset supplement!' + str(oe))
            # this is an error - the attribute must be present to determine how to append the data supplement time coordinate!


        # Get the end time of the supplement
        try:
            string_time = sup_root.FindAttributeByName('ion_time_coverage_end')
            sup_etime = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
            log.debug('Supplement End Time: %s (%i)' % (string_time.GetValue(), sup_etime))

        except OOIObjectError, oe:
            raise IngestionError('No end time attribute found in dataset supplement!' + str(oe))
            # this is an error - the attribute must be present to determine how to append the data supplement time coordinate!
        
        
        result.update({EM_START_DATE:sup_stime*1000,
                       EM_END_DATE:sup_etime*1000})
        
        
        
        # Get the start/end time indices of the current dataset and applicable offsets
        cur_etime = None
        is_new_ds = False
        try:
            string_time = cur_root.FindAttributeByName('ion_time_coverage_end')
            cur_etime = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
            log.debug('Current End Time:      %s (%i)' % (string_time.GetValue(), cur_etime))
            
        except OOIObjectError, oe:
            is_new_ds= True
            log.debug(oe)
            log.info('Aggregation offset unchanged - dataset has no ion_time_coverage_end.')
            # This is not an error - it is a new dataset.
            # ATTENTION: Is there any benefit to determining if there is a gap in the sequence here?


        return_value = (cur_root, cur_etime, cur_agg_dim_length, sup_root, sup_stime, sup_etime, sup_agg_dim_length, sup_agg_dim_name, sup_fcst_dim_name, is_new_ds, result)
        defer.returnValue(return_value)


    @defer.inlineCallbacks
    def __calculate_merge_offsets(self, is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_stime, sup_etime, cur_etime, cur_agg_dim_length, sup_agg_dim_length, sup_fcst_dim_name):
        
        sup_sindex       = cur_agg_dim_length
        sup_eindex       = cur_agg_dim_length
        insertion_offset = sup_agg_dim_length     #- Net change to the dataset along the aggregation dimension (how the length of the dim changes)
                                                  #  defaults to length so that if there are no overlaps (during data append)
                                                  #  we adjust the dimension length by adding this value.. otherwise, this
                                                  #  var will be replaced so that it represents the differences between the
                                                  #  number of values being inserted and the number of values that insertion
                                                  #  would replace.
                                                  #    Example 1: When appending data which doesn't overwrite:
                                                  #          insertion_offset = (length - 0) = length
                                                  #    Example 2: When overwriting identical data:
                                                  #          insertion_offset = (length - length = 0
                                                  #    Example 3: (insetion) When 5 values overwrite 4 values:
                                                  #          insertion =_offset = (5 - 4) = 1
        runtime_offset_seconds   = 0               # @todo: Add notes here
        forecast_offset_seconds  = 0               # @todo: Add notes here
        
        # If cur_etime is None then the current dataset does not exist.
        # In this case the cur_agg_dim_length is 0 -- and therefore all our offsets will be as well
        # insertion_offset will be the sup_agg_dim_length, which is equal to the total number of values we are inserting
        if cur_etime is None:
            log.info('__calculate_merge_offsets(): Current dataset does not exist -- offsets will be based on a starting index of 0')
            
        else:
            log.info('__calculate_merge_offsets(): Current dataset exists, moving onward...')
            sup_agg_var = None
            cur_agg_var = None
            
            # First calculate runtime and forecast offsets (these will affect the other offsets)
            if sup_fcst_dim_name is not None and not len(sup_fcst_dim_name) == 0:
                log.info('__calculate_merge_offsets(): Calculating runtime and forecast time (normalization) offsets for FMRC dataset...')
                
                # Get the runtime and forecast time from dataset and supplement
                log.debug('__calculate_merge_offsets(): Retrieving time variables from dataset and supplement...')
                try:
                    cur_agg_var  = cur_root.FindVariableByName(sup_agg_dim_name)
                    cur_fcst_var = cur_root.FindVariableByName(sup_fcst_dim_name)
                except OOIObjectError, ex:
                    raise IngestionError('__calculate_merge_offsets(): FMRC Dataset must have (model) Run Time and Forecast Time dimension, respectivly named ("%s", "%s").  Inner Exception: %s' % (sup_agg_dim_name.encode('utf-8'), sup_fcst_dim_name.encode('utf-8'), str(ex)))
                    
                try:
                    sup_agg_var  = sup_root.FindVariableByName(sup_agg_dim_name)
                    sup_fcst_var = sup_root.FindVariableByName(sup_fcst_dim_name)
                except OOIObjectError, ex:
                    raise IngestionError('__calculate_merge_offsets(): FMRC Supplement must have (model) Run Time and Forecast Time dimension, respectivly named ("%s", "%s").  Inner Exception: %s' % (sup_agg_dim_name.encode('utf-8'), sup_fcst_dim_name.encode('utf-8'), str(ex)))
                
                # Build a list containing a tuple of time variable name, units string, units base time (millis)
                log.debug('__calculate_merge_offsets(): Building a list of tuples with (time variable name, units string) for each time variable...')
                units_list = [] 
                units_list.append((sup_agg_var.name, sup_agg_var.GetUnits()))
                units_list.append((cur_agg_var.name, cur_agg_var.GetUnits()))
                units_list.append((sup_fcst_var.name, sup_fcst_var.GetUnits()))
                units_list.append((cur_fcst_var.name, cur_fcst_var.GetUnits()))
                
                
                log.debug('__calculate_merge_offsets():  Calculating units base time for each time variable...')
                import re
                time_units_regex = 'hours since ([\d]{4}-[\d]{2}-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}Z)'
                
                for i in range(len(units_list)):
                    vname = units_list[i][0]
                    units = units_list[i][1]
                    m = re.search(time_units_regex, units)
                    
                    if m is None:
                        raise IngestionError('Units attribute for time variable "%s" must be "hours since yyyy-MM-ddTHH:mm:ssZ" to perform an FMRC aggregation' % units_list[i][0].encode('utf-8'))
                    
                    base_time_millis = calendar.timegm(time.strptime(m.group(1), '%Y-%m-%dT%H:%M:%SZ'))
                    
                    units_list[i] = (vname, units, base_time_millis)
                    log.debug('__calculate_merge_offsets(): Adding tuple values to variable units list: %s' % str(units_list[i]))
                
                # Determine the difference between the units in the dataset and supplement for runtime and forecast time
                runtime_offset_seconds  = units_list[0][2] - units_list[1][2]
                forecast_offset_seconds = units_list[2][2] - units_list[3][2]
                
                # If the dataset is an FMRC the units of the dataset and supplement may differ -- adding runtime_offset_seconds here
                #   accomodates for this case by normalizing the supplements start and end time to be based on the same units as the
                #   the current dataset.
                sup_stime += runtime_offset_seconds
                sup_etime += runtime_offset_seconds    
                
            if is_overwrite:
                if cur_etime < sup_stime:
                    log.info('__calculate_merge_offsets(overwrite): Supplement does not overlap.')
                    
                else:
                    log.info('__calculate_merge_offsets(overwrite): Supplement overlaps -- calculating offsets')
                    # Find where the supplement start time and end time should lay in the dataset (supplement_sindex, supplement_eindex)
                    # Find how the origins in the dataset's data should change according to this position (insertion_offset)
                    if cur_agg_var is None:
                        cur_agg_var = cur_root.FindVariableByName(sup_agg_dim_name)
                    time_indices = yield self._find_time_index(cur_agg_var, [sup_stime, sup_etime])
                    log.debug('time_indices = %s' % str(time_indices))
                    
                    sup_sindex = time_indices[sup_stime]
                    sup_eindex = time_indices[sup_etime]
                    
                    # Adjust indices when the supplement times lay between indices in the current dataset
                    if sup_sindex < 0:
                        sup_sindex = -sup_sindex - 1
                    if sup_eindex < 0:
                        sup_eindex = -sup_eindex - 2
                        
                        
                    # Calculate the insertion offset -- how bounded_arrays ordered after the overwritten section must be offset
                    supplement_impact = (sup_eindex + 1) - sup_sindex  #(number of values displaced by inserting this supplement into the dataset)
                    insertion_offset  = sup_agg_dim_length - supplement_impact
            
            else:
    
                # For overlap - determine how many indices overlap to determine our offsets
                overlap_count = 0
                
                if cur_etime == sup_stime:
                    overlap_count = 1
                    log.info('__calculate_merge_offsets(overlap): Supplement overlaps by 1')
    
                elif cur_etime > sup_stime:
                    log.info('__calculate_merge_offsets(overlap): Supplement overlaps -- calculating number of overlaps')
                    if sup_agg_var is None:
                        sup_agg_var = sup_root.FindVariableByName(sup_agg_dim_name)
                    time_indices = yield self._find_time_index(sup_agg_var, [cur_etime - runtime_offset_seconds])
                    time_index   = time_indices[cur_etime - runtime_offset_seconds]
                    
                    # Overlap has some restrictions--
                    if time_index < 0:
                        if time_index == -(sup_agg_dim_length + 1):
                            raise IngestionError('Current data overlaps the supplement entirely and must be overwritten!  Supplement will NOT be merged!  index:%i' % time_index)
                        elif time_index == -1:
                            raise IngestionError('Supplement data does not actualy overlap the current data!  Supplement will NOT be merged!  index:%i' % time_index)
                        else:
                            raise IngestionError('Overlapping supplement data does not match current data!  Supplement will NOT be merged!  index:%i' % time_index)
                    
                    overlap_count = time_index + 1
                    
                else:
                    log.info('__calculate_merge_offsets(overlap): Supplement does not overlap.')
            
            
                insertion_offset -= overlap_count
                sup_sindex       -= overlap_count
        

        result = (sup_sindex, sup_eindex, insertion_offset, int(runtime_offset_seconds / 3600), int(forecast_offset_seconds / 3600))
        defer.returnValue(result)


    @defer.inlineCallbacks
    def __merge(self, is_overwrite, cur_root, sup_root, sup_agg_dim_name, sup_fcst_dim_name, sup_stime, cur_etime, sup_sindex, sup_eindex, insertion_offset, runtime_offset, forecast_offset, is_new_ds):
        result = {}
        
        ###
        ### Add the dimensions from the supplement to the current state if they are not already there
        ###
        merge_dims = {}
        for merge_dim in sup_root.dimensions:
            merge_dims[merge_dim.name] = merge_dim

        dims = {}
        for dim in cur_root.dimensions:
            dims[dim.name] = dim

        if merge_dims.keys() != dims.keys():
            if len(dims) != 0:
                raise IngestionError('Can not ingest supplement with different dimensions than the dataset')
            else:
                # ! New dataset -- add all dimensions
                for merge_dim in sup_root.dimensions:
                    dim_link = cur_root.dimensions.add()
                    dim_link.SetLink(merge_dim)

        else:
            # ! We are appending an existing dataset - adjust the length of the aggregation dimension
            agg_dim = dims[sup_agg_dim_name]
            agg_dim.length += insertion_offset
            log.info('Setting the aggregation dimension %s to %d' % (agg_dim.name, agg_dim.length))


        ###
        ### Adjust the values of the model time and forecast time in the supplement if this is FMRC:
        ###
#        log.debug('before FMRC time var normalize')
#        if runtime_offset is not 0:
#            runtime_var = sup_root.FindVariableByName(sup_agg_dim_name)
#            need_keys = [ba.GetLink('ndarray').key for ba in runtime_var.content.bounded_arrays]
#            yield self._fetch_blobs(cur_root.Repository, need_keys)
#
#            # @attention:
#            # @bug:       The following lines fail because the merge dataset cannot be modified, we have to figure out a way to modify these values before merging
#            # @attention:
#            for ba in runtime_var.content.bounded_arrays:
#                for i in range(len(ba.ndarray.value)):
#                    ba.ndarray.value[i] += runtime_offset
                    
#        if forecast_offset is not 0:
#            forecast_var = sup_root.FindVariableByName(sup_fcst_dim_name)
#            need_keys = [ba.GetLink('ndarray').key for ba in forecast_var.content.bounded_arrays]
#            yield self._fetch_blobs(cur_root.Repository, need_keys)
#
#            for ba in forecast_var.content.bounded_arrays:
#                for i in range(len(ba.ndarray.value)):
#                    ba.ndarray.value[i] += forecast_offset
                    
        
        ###
        ### Add/merge the variables from the supplement to the current state if they are not already there
        ### Merge variable if it is dimensioned on the aggregation dimension (sup_agg_dim)
        ###
        agg_dim_min_offset = 0
        if not is_new_ds:
            agg_dim_min_offset = dims[sup_agg_dim_name].min_offset
        sup_sindex += agg_dim_min_offset            # The start position of the supplement relative to the dataset must be offset by the starting index of the dataset
        sup_eindex += agg_dim_min_offset            # The end position of the supplement relative to the dataset must be offset by the starting index of the dataset
        log.debug('>> agg_dim_min_offset    = %i' % agg_dim_min_offset)
        log.debug('>> sup     min_offset    = %i' % merge_dims[sup_agg_dim_name].min_offset)
        log.debug('>> sup_sindex (adjusted) = %i' % sup_sindex)
        log.debug('>> sup_eindex (adjusted) = %i' % sup_eindex)
        for merge_var in sup_root.variables:
            var_name = merge_var.name

            log.info('Merge Var Name: %s' % merge_var.name)

            
            # Step 1: Copy new variables from the supplement into the dataset
            try:
                var = cur_root.FindVariableByName(var_name)
            except OOIObjectError, oe:
                log.debug(oe)
                
                if is_new_ds:
                    log.info('Variable %s does not yet exist in the dataset!' % var_name)
    
                    v_link = cur_root.variables.add()
                    v_link.SetLink(merge_var, ignore_copy_errors=True)
    
                    log.info('Copied Variable %s into the dataset!' % var_name)
                    continue # Go to next variable...
                else:
                    raise IngestionError('Variable %s does not exist in the dataset.  Supplement is invalid!' % var_name)


            # Step 2: Skip merge for variables which are not dimensioned on the sup_agg_dim
            # @todo: check to see if supplement shape and dataset shape don't match (like if time dimensions are at different indices)
            merge_agg_dim_idx = -1
            for i in range(len(merge_var.shape)):
                if merge_var.shape[i].name == sup_agg_dim_name:
                    merge_agg_dim_idx = i
                    break
            else:
                log.info('Nothing to merge on variable %s which does not share the aggregation dimension' % var_name)
                continue # Ignore this variable...


            # @todo: check attributes for variables which are not aggregated....

            
            # Step 3: Merge variable values
            # If this is a new dataset applying all these offsets is not necessary
            if is_overwrite and not is_new_ds:
                # Step 3a: (overwrite) Restructure the data in the dataset
                if cur_etime >= sup_stime:
                    #          This requires the following:
                    #            1) Removing bounded arrays which are totally contained in the supplement
                    #            2) Breaking apart bounded arrays which contain 1 or more overlapping indices
                    #               and throwing away the overlapping region
                    #            3) Adjusting the indices of all bounded_arrays positioned after the overwrite
                    for i in reversed(range(len(var.content.bounded_arrays))):  # Iterate in reverse so that we can add and delete items without affecting our indices
                        ba = var.content.bounded_arrays[i]
                        bound = ba.bounds[merge_agg_dim_idx]
                        
                        log.debug('Var:"%s"  BA:%i  BA.origin:%i  BA.size:%i  sup_sindex:%i  sup_eindex:%i' % (var.name, i, bound.origin, bound.size, sup_sindex, sup_eindex))
        
                        # (contains check)
                        # @todo: add unit tests for this case!
                        if bound.origin >= sup_sindex and bound.origin + bound.size - 1 <= sup_eindex:
                            # Remove this bounded array
                            if log.getEffectiveLevel() <= logging.DEBUG:
                                log.debug('(contains) Removing bounded array from variable "%s" [index:%i, origin:(%s), size:(%s)]' % ( \
                                                                                                                                        var.name.encode('utf-8'), \
                                                                                                                                        i, \
                                                                                                                                        str([bbb.origin for bbb in ba.bounds]), \
                                                                                                                                        str([bbb.size for bbb in ba.bounds]), \
                                                                                                                                      ))
                            del var.content.bounded_arrays[i]
                        
                        # (intersects check)
                        # @todo: add unit tests for this case!
                        elif bound.origin <= sup_eindex and bound.origin + bound.size > sup_sindex:
                            # Split the bounded array according to what data overlaps
                            
                            # if the supplement starts after this bounded_array's origin...
                            if sup_sindex > bound.origin:
                                # Create a new bounded_array containing only values leading up to the supplement (along the agg dimension)
                                new_ba = yield self.subset_bounded_array(var.Repository, ba, merge_agg_dim_idx, bound.origin, sup_sindex)
                                ba_link = var.content.bounded_arrays.add()
                                ba_link.SetLink(new_ba)
                                if log.getEffectiveLevel() <= logging.DEBUG:
                                    log.debug('(intersects) Split bounded array for variable "%s".  Created [left] [index:%i, origin:(%s), size:(%s)]' % ( \
                                                                                                                                                           var.name.encode('utf-8'), \
                                                                                                                                                           i, \
                                                                                                                                                           str([bbb.origin for bbb in new_ba.bounds]), \
                                                                                                                                                           str([bbb.size for bbb in new_ba.bounds]), \
                                                                                                                                                         ))
                            
                            if sup_eindex < (bound.origin + bound.size) - 1:
                                log.debug('here')
                                # Create a new bounded_array containing only values from the end of the supplement to the end of the existing bounded_array (along the agg dim)
                                new_ba = yield self.subset_bounded_array(var.Repository, ba, merge_agg_dim_idx, sup_eindex + 1, bound.origin + bound.size)
                                ba_link = var.content.bounded_arrays.add()
                                ba_link.SetLink(new_ba)
                                if log.getEffectiveLevel() <= logging.DEBUG:
                                    log.debug('(intersects) Split bounded array for variable "%s".  Created [right] [index:%i, origin:(%s), size:(%s)]' % ( \
                                                                                                                                                            var.name.encode('utf-8'), \
                                                                                                                                                            i, \
                                                                                                                                                            str([bbb.origin for bbb in new_ba.bounds]), \
                                                                                                                                                            str([bbb.size for bbb in new_ba.bounds]), \
                                                                                                                                                          ))
                            
                            # Either way remove this bounded array
                            if log.getEffectiveLevel() <= logging.DEBUG:
                                log.debug('(intersects) Removing bounded array from variable "%s" [index:%i, origin:(%s), size:(%s)]' % ( \
                                                                                                                                          var.name.encode('utf-8'), \
                                                                                                                                          i, \
                                                                                                                                          str([bbb.origin for bbb in ba.bounds]), \
                                                                                                                                          str([bbb.size for bbb in ba.bounds]), \
                                                                                                                                        ))
                            del var.content.bounded_arrays[i]

                        
                        # (post insertion check)
                        # @todo: add unit tests for this case!
                        elif bound.origin > sup_eindex:
                            # Adjust the origin using insertion_offset
                            log.debug("(post-insert) Adjusting bound.origin from %i to %i" % (bound.origin, bound.origin + insertion_offset))
                            bound.origin += insertion_offset
            
            
            # Step 3b: Normalize the supplement origins to zero
            # Since the supplement is ReadOnly calculate the offset here.. and apply to the dataset afterwards
            merge_var_min_origin = 0
            if not is_new_ds:
                # Replace with Dim - min_offset?
                merge_var_min_origin = float('inf')
                for ba in merge_var.content.bounded_arrays:
                    merge_var_min_origin = min(merge_var_min_origin, ba.bounds[merge_agg_dim_idx].origin)
                log.debug('>> merge_var_min_origin = %i' % merge_var_min_origin)
            
            
            # Step 3c: Merge the supplement into the dataset
            new_bas = []
            for merge_ba in merge_var.content.bounded_arrays:
                ba = var.Repository.copy_object(merge_ba, deep_copy=False)
                ba.bounds[merge_agg_dim_idx].origin += sup_sindex - merge_var_min_origin

                ba_link = var.content.bounded_arrays.add()
                ba_link.SetLink(ba)

                # Keep track of these in case we need to adjust the values
                new_bas.append(ba)

            log.debug('before FMRC time var normalize')
            if runtime_offset is not 0 and var_name == sup_agg_dim_name:

                need_keys = [ba.GetLink('ndarray').key for ba in merge_var.content.bounded_arrays]
                yield self._fetch_blobs(cur_root.Repository, need_keys)

                # @attention:
                # @bug:       The following lines fail because the merge dataset cannot be modified, we have to figure out a way to modify these values before merging
                # @attention:
                for ba in new_bas:
                    for i in range(len(ba.ndarray.value)):
                        ba.ndarray.value[i] += runtime_offset


            if forecast_offset is not 0 and var_name == sup_fcst_dim_name:

                need_keys = [ba.GetLink('ndarray').key for ba in merge_var.content.bounded_arrays]
                yield self._fetch_blobs(cur_root.Repository, need_keys)

                for ba in new_bas:
                    for i in range(len(ba.ndarray.value)):
                        ba.ndarray.value[i] += forecast_offset



            log.info('Merged Variable %s into the dataset!' % var_name)

            
            # Step 4: Merge variable attributes
            merge_att_ids = set()
            for merge_att in merge_var.attributes:
                merge_att_ids.add(merge_att.MyId)

            att_ids = set()
            for att in var.attributes:
                att_ids.add(att.MyId)

            if att_ids != merge_att_ids:

                for merge_att in merge_var.attributes:
                    log.error('Merge Att: %s, %s, %s' % (merge_att.name, str(merge_att.GetValue()), base64.encodestring(merge_att.MyId)[0:-1]))

                for att in var.attributes:
                    log.error('Att: %s, %s, %s' % (att.name, str(att.GetValue()), base64.encodestring(att.MyId)[0:-1]))

                #@TODO turn this error detection back on!
                #raise ImportError('Variable %s attributes are not the same in the supplement!' % var_name)



        ###
        ### Add/merge the attributes from the supplement to the current state
        ###
        # @TODO Get the vertical positive 'direction!' Deal with attributes accordingly.

        try:
            merge_att = sup_root.FindAttributeByName('ion_geospatial_vertical_positive')
            merge_vertical_positive = merge_att.GetValue()
        except OOIObjectError, oe:
            log.debug(oe)
            merge_vertical_positive = None

        try:
            att = cur_root.FindAttributeByName('ion_geospatial_vertical_positive')
            vertical_positive = att.GetValue()
        except OOIObjectError, oe:
            log.debug(oe)
            vertical_positive = None

        if merge_vertical_positive is not None and vertical_positive is not None:
            if merge_vertical_positive != vertical_positive:
                raise IngestionError('Can not merge a data supplement that switches vertical positive!')

        else:
            # Take which ever one is not None
            vertical_positive = vertical_positive or merge_vertical_positive


        for merge_att in sup_root.attributes:

            att_name = merge_att.name
            try:


                log.info('Merging Attribute: %s' % att_name)

                if att_name == 'ion_time_coverage_start':
                    cur_root.MergeAttLesser(att_name, sup_root)

                elif att_name == 'ion_time_coverage_end':
                    cur_root.MergeAttGreater(att_name, sup_root)

                elif att_name == 'ion_geospatial_lat_min':
                    cur_root.MergeAttLesser(att_name, sup_root)

                elif att_name == 'ion_geospatial_lat_max':
                    cur_root.MergeAttGreater(att_name, sup_root)
                
                elif att_name == 'ion_geospatial_lon_min':
                    # @TODO Need a better method to merge these - determine the greater extent of a wrapped coordinate
                    cur_root.MergeAttSrc(att_name, sup_root)

                elif att_name == 'ion_geospatial_lon_max':
                    # @TODO Need a better method to merge these - determine the greater extent of a wrapped coordinate
                    cur_root.MergeAttSrc(att_name, sup_root)


                elif att_name == 'ion_geospatial_vertical_min':
                    
                    # Check vert vmin/vmax for NaN, either is NaN or missing, don't merge
                    vmin = False
                    vmax = False
                    if sup_root.HasAttribute('ion_geospatial_vertical_min'):
                        val = sup_root.FindAttributeByName('ion_geospatial_vertical_min').GetValue()
                        if not pu.isnan(val):
                            vmin = True
                    
                    # Only check for vmax if vmin is available (if either value is not available, we can't continue)
                    if vmin is not None:
                        if sup_root.HasAttribute('ion_geospatial_vertical_max'):
                            val = sup_root.FindAttributeByName('ion_geospatial_vertical_max').GetValue()
                            if not pu.isnan(val):
                                vmax = True
                    
                    
                    if vmin and vmax:
                        if vertical_positive == 'down':
                            cur_root.MergeAttLesser(att_name, sup_root)
    
                        elif vertical_positive == 'up':
                            cur_root.MergeAttGreater(att_name, sup_root)
    
                        else:
                            raise OOIObjectError('Invalid value for Vertical Positive but ion_geospatial_vertical_min is present')
                    else:
                        root_min = cur_root.HasAttribute('ion_geospatial_vertical_min')
                        root_max = cur_root.HasAttribute('ion_geospatial_vertical_max')
                        
                        # if cur_root doesnt have vmin/vmax, add new attributes with default values...
                        if not root_min and not root_max:
                            cur_root.AddAttribute('ion_geospatial_vertical_min', cur_root.DataType.DOUBLE, float('nan'))
                            cur_root.AddAttribute('ion_geospatial_vertical_max', cur_root.DataType.DOUBLE, float('nan'))


                elif att_name == 'ion_geospatial_vertical_max':

                    # Check vert vmin/vmax for NaN, either is NaN or missing, don't merge
                    vmin = False
                    vmax = False
                    if sup_root.HasAttribute('ion_geospatial_vertical_min'):
                        val = sup_root.FindAttributeByName('ion_geospatial_vertical_min').GetValue()
                        if not pu.isnan(val):
                            vmin = True
                    
                    # Only check for vmax if vmin is available (if either value is not available, we can't continue)
                    if vmin is not None:
                        if sup_root.HasAttribute('ion_geospatial_vertical_max'):
                            val = sup_root.FindAttributeByName('ion_geospatial_vertical_max').GetValue()
                            if not pu.isnan(val):
                                vmax = True
                    
                    
                    if vmin and vmax:
                        if vertical_positive == 'down':
                            cur_root.MergeAttGreater(att_name, sup_root)
    
                        elif vertical_positive == 'up':
                            cur_root.MergeAttLesser(att_name, sup_root)
    
                        else:
                            raise OOIObjectError('Invalid value for Vertical Positive but ion_geospatial_vertical_max is present')
                    else:
                        root_min = cur_root.HasAttribute('ion_geospatial_vertical_min')
                        root_max = cur_root.HasAttribute('ion_geospatial_vertical_max')
                        
                        # if cur_root doesnt have vmin/vmax, add new attributes with default values...
                        if not root_min and not root_max:
                            cur_root.AddAttribute('ion_geospatial_vertical_min', cur_root.DataType.DOUBLE, float('nan'))
                            cur_root.AddAttribute('ion_geospatial_vertical_max', cur_root.DataType.DOUBLE, float('nan'))
                        


                elif att_name == 'history':
                    # @TODO is this the correct treatment for history?
                    cur_root.MergeAttDstOver(att_name, sup_root)

                else:
                    cur_root.MergeAttSrc(att_name, sup_root)

            except OOIObjectError, oe:

                log.exception('Attribute merger failed for global attribute: %s' % att_name)
                result[EM_ERROR] = 'Error during ingestion of global attributes'
            
            except ValueError, ex:
                
                log.exception('Attribute merger failed for global attribute "%s".  Cause: %s' % (att_name, str(ex)))


        defer.returnValue(result)
            


    def _find_time_var(self, group):

        time_vars = []

        for var in group.variables:

            #Parse the atts - try to short cut logic to identify time...
            for att in var.attributes:

                if att.name == 'standard_name' and att.GetValue() == 'time':
                    log.debug('Found standard name "time" in variable named: %s' % var.name)

                    time_vars.append(var)
                    break # Don't continue to iterate attributes

                elif att.name == 'units' and att.GetValue().find(' since ') != -1:
                    log.debug('Found units att with "since" in variable named: %s' % var.name)

                    time_vars.append(var)
                    break # Don't continue to iterate attributes
                
#                elif att.name == '_CoordinateAxisType' and (att.GetValue() == 'Time' or att.GetValue() =='RunTime'):
#                    log.debug('Found _CoordinateAxisType "%s" in variable name: %s' % (att.GetValue().encode('utf-8'), var.name.encode('utf-8')))
#                    
#                    time_vars.append(var)
#                    break # Don't continue to iterate attributes
                    

            else:
                log.debug('Variable named "%s" does not appear to be a time variable.' % var.name)

        # Do not raise an exception here if none are found - let the ingestion method handle the issue based on the return

        return time_vars

    @classmethod
    def _get_ndarray_keys(cls, dataset_variable):
        """
        @Brief: Retrieves all the ID reference keys for the ndarrays contained in all the bounded_arrays of the given variable.
                This method is useful to acquire the keys needed in order to fetch blobs from the dataset since, by default,
                the ndarrays of variables are excluded when datasets are checked out.
        """
        results = []
        for ba in dataset_variable.content.bounded_arrays:
            results.append(ba.GetLink('ndarray').key)
        return results
    
    @classmethod
    def _get_ndarray_vals(cls, time_variable):
        """
        @Brief: Retrieves all the values of all the bounded arrays in the given time_variable
        @return: A sorted list of tuples containing the values index followed by the value at that index.
                 Since a variable's bounded array's may contain duplicate data, this array may contain
                 tuples which specify the same first value.  When this is the case, it is useful to
                 ensure that the two values specified at the same index match, otherwise the variable
                 is corrupt.  NOTE: This validation is NOT accomplished by this method.
                 
        @note: This method will not yet work on multidimensional variables (more than one item in the
               bounded array's list of bounds).  This is because iteration over such a structure is
               quite complicated and currently unnessary since this method is only used for time
               variables (true time coordinate variables will only ever have one dimension -- time)
               
        @note: This method assumes all blobs for the components of the given time_variable (bounded_arrays,
               ndarrays, etc) have been fetched.  If they have not it will fail with a KeyError.
        """
        results = []
        for ba in time_variable.content.bounded_arrays:
            if len(ba.bounds) > 1:
                raise IngestionException('_get_ndarray_vals does not support enflating bounded arrays with more than one dimension -- yet')
            origin = ba.bounds[0].origin
            for i in range(len(ba.ndarray.value)):
                val = ba.ndarray.value[i]
                idx = origin + i
                results.append((idx, val))
        results.sort()
        return results


    @defer.inlineCallbacks
    def _fetch_blobs(self, repo, fetch_keys):
        """
        Helper method to fetch blobs from the Datastore
        
        Determines which keys in fetch_keys are not already loaded in the given repositories index_hash and then
        fetches the blobs for those remaining keys from the datastore.  The repositories index_hash is then
        updated as expected
        
        @attention: This is more of an 'exgest' facility.  Should this method be moved to a move cohesive location (datastore)?
        @attention: Consider exposing and using datastore.DataStoreWorkbench._get_blobs() instead of this method
        """
        if repo is None:
            raise IngestionError('Cannot fetch blobs for a non-existant repository')

        # Find which keys are needed (those not already in the repository's index_hash)
        repo_keys = repo.index_hash.keys()
        need_keys = set(fetch_keys).difference(repo_keys)

#                        workbench_keys = set(self._workbench_cache.keys())
#                        local_keys = workbench_keys.intersection(need_keys)

#                        for key in local_keys:
#                        for key in need_keys:
#                            try:
#                                repo.index_hash.get(key)
#                                need_keys.remove(key)
#                            except KeyError, ke:
#                                log.info('Key disappeared - get it from the remote after all')
                
        if len(need_keys) > 0:
            blobs_request = yield self.mc.create_instance(BLOBS_REQUSET_MESSAGE_TYPE)
            blobs_request.blob_keys.extend(need_keys)

            try:
                blobs_msg = yield self.dsc.fetch_blobs(blobs_request)
            except ReceivedError, re:
               log.debug('ReceivedError', str(re))
               raise IngestionError('Could not fetch ndarray blobs from the datastore during merge!  Cause: "%s"' % re.msg_content)


            for se in blobs_msg.blob_elements:
                # Put the new objects in the repository
                element = gpb_wrapper.StructureElement(se.GPBMessage)
                repo.index_hash[element.key] = element


    @defer.inlineCallbacks
    def _find_time_index(self, time_var, search_times, THRESHOLD = 0.001):
        """
        @todo: Add documentation about what negative value returns mean
        """
        search_times_cpy = search_times[:]  # copy the search_times list so we can remove values
        # Step 1: Get a handle to an array of values from the given time variable
        # Step 1a: Gather a list of keys for blobs which need to be fetched             
        
        log.debug('Gathering a list of keys for blobs which need to be fetched...')
        need_keys = IngestionService._get_ndarray_keys(time_var)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('>>  (ndarray) need_keys = %s' % str(need_keys))
        
        # Step 1b: Fetch all blobs for the ndarrays in the time_var
        log.debug('Fetching all blobs for the ndarrays in the supplement...')
        repo = self.dataset.Repository
        yield self._fetch_blobs(repo, need_keys)

        # Step 1c: Now, grab all the array values from the ndarrays..
        log.debug('Grabbing all the array values from the ndarrays...')
        values = IngestionService._get_ndarray_vals(time_var)
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('>>  ndarray values = %s' % str(values))


        # Step 2: Perform a linear search in the values array for each time in search_times
        #         a) Binary search will most likely be performed nearer worse case (logN time) - since the
        #            overlapping region should (on average) be fairly small
        #            * Linear search may be optimal for average case
        #            ** Linear search is "required" when indexing into the current data (rather than the
        #               supplement) because we must traverse multiple bounded arrays
        
        
        # Step 2a: Perform the linear search to see where in the set of values our search_times lay
        log.debug('Searching for values "%s" in ndarray list' % str(search_times_cpy))
        results_dict = {}
        for i in range(len(values)):
            idx, val = values[i]
            # @note: this is a good place to check if two entries in the time variables bounded_arrays
            #        contain mismatched values for the same index -- this shouldn't happen however
            for search_time in search_times_cpy:
                if val is search_time or abs(val - search_time) < THRESHOLD: # TODO: should change this to using some sort of near-match function
                    results_dict[search_time] = idx
                    search_times_cpy.remove(search_time)
                elif search_time < val:
                    results_dict[search_time] = -(i + 1)
                    search_times_cpy.remove(search_time)
                # else search_time > val: continue
            if len(search_times_cpy) == 0:
                break; 
                
        if len(search_times_cpy) > 0:
            not_in_list_val = -(len(values) + 1)
            for search_time in search_times_cpy:
                results_dict[search_time] = not_in_list_val
        # don't use the copy of the list after this point -- structure is indeterminant
        search_times_cpy = None
                
            
        # Step 2b: @todo: Make sure that we aren't experiencing a roundoff issue by checking how close
        #           our search_start is to the values before and after it in the list (floats only)
         

        defer.returnValue(results_dict)


    @defer.inlineCallbacks
    def subset_bounded_array(self, repo, old_ba, idx, min, max):
        """
        Creates a new bounded array which is a subset of the given bounded_array.  "repo" is used to construct
        the resultant object but it is the callers responsibility to attach the bounded array to something
        (such as a variable)
        
        @param repo: The repository in which the bounded_array subset will be created
        @param old_ba: The bounded array from which a new subset will be created
        @param idx: The index of the subset dimension in var.bounds
        @param min: The minimum index for the subset (inclusive) along the subset dimension (idx)
        @param max: The maximum index for the subset (exclusive) along the subset dimension (idx)
        
        @note: When determining if the index of an element in the bounded_array lies within the range
               given by min and max, the origin of that bounded_array is applied first.  As such,
               min and max should specify canonical indices of data values in the datastructure itself
               rather than specifying indices of values in the given bounded_array's underlying ndarray.
               
               Example:
                 For a 1-D bounded array with: origin=12; size=5; values=[10,11,12,13,14]
                 
                 yield subset_bounded_array(..., idx=0, min=1,  max=4)    returns nothing
                 yield subset_bounded_array(..., idx=0, min=13, max=16)   returns BA [11,12,13]
               
        """
        new_ba = repo.create_object(CDM_BOUNDED_ARRAY_TYPE)
        
        # Step 1: Fetch blobs for this bounded_array
        need_keys = [old_ba.GetLink('ndarray').key]
        yield self._fetch_blobs(repo, need_keys)
        
        # Step 1: Create the bounds
        old_shape = []
        for i in range(len(old_ba.bounds)):
            old_bound = old_ba.bounds[i]
            new_bound = new_ba.bounds.add()
            
            old_shape.append(old_bound.size) # (used in step 2 below)
            
            if i == idx:
                new_bound.size = max - min
                new_bound.origin = min
            else:
                new_bound.size = old_bound.size
                new_bound.origin = old_bound.origin
            new_bound.stride = old_bound.stride # not used here, but must transpose
        
        
        # Step 2: Create the ndarray
        old_nd = old_ba.ndarray
        new_nd = repo.create_object(old_nd.ObjectType)
        
        offset_min = min - old_ba.bounds[idx].origin
        offset_max = max - old_ba.bounds[idx].origin
        for i in range(len(old_nd.value)):  # (.value is a list)
            shape = variables._unflatten_index(i, old_shape)
            
            # If this value is in our subset range (for the dimension indexed by idx)
            if (shape[idx] >= offset_min and shape[idx] < offset_max):
                new_nd.value.append(old_nd.value[i])
        
        
        # Make sure the actual length of the new ndarray matches the length indicated by
        # the cumulative sizes of the arrays bounds
        expect_len = reduce(lambda x,y: x+y, [bound.size for bound in new_ba.bounds])
        actual_len = len(new_nd.value)
        assert(expect_len == actual_len, 'Actual length of ndarray != expected length: %i != %i' % (actual_len, expect_len))


        
        new_ba.ndarray = new_nd
        defer.returnValue(new_ba)
        
        


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
    def ingest(self, msg):
        """
        Start the ingest process by passing the Service a topic to communicate on, a
        routing key for intermediate replies (signaling that the ingest is ready), and
        a custom timeout for the ingest service (since it may take much longer than the
        default timeout to complete an ingest)
        @param msg, GPB 2002/1, a PerformIngestMessage
        @retval Result is an empty ION Message, reply_ok
        @GPB{Input,2002,1}
        """
        log.debug('-[]- Entered IngestionClient.ingest()')
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()

        ingest_service_timeout = msg.ingest_service_timeout

        # Invoke [op_]() on the target service 'dispatcher_svc' via RPC
        log.info("@@@--->>> Sending 'ingest' RPC message to ingestion service")
        (content, headers, msg) = yield self.rpc_send('ingest', msg, timeout=ingest_service_timeout + 30)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def create_dataset_topics(self, msg):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_dataset_topics', msg)
        defer.returnValue(content)

    """
    @defer.inlineCallbacks
    def send_dataset(self, topic, msg):
        ''' For testing the service...'''
        yield self._check_init()
        yield self.proc.send(topic, operation='recv_dataset', content=msg)

    @defer.inlineCallbacks
    def send_chunk(self, topic, msg):
        ''' For testing the service...'''
        yield self._check_init()
        yield self.proc.send(topic, operation='recv_chunk', content=msg)

    @defer.inlineCallbacks
    def send_done(self, topic, msg):
        ''' For testing the service...'''
        yield self._check_init()
        yield self.proc.send(topic, operation='recv_done', content=msg)
    """

# Spawn of the process using the module name
factory = ProcessFactory(IngestionService)
