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

import base64
import pprint

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceClientError
from ion.services.dm.distribution.publisher_subscriber import Subscriber, PublisherFactory

from ion.core.object.cdm_methods import attribute_merge

from ion.core.exception import ApplicationError

# For testing - used in the client
from ion.services.dm.distribution.pubsub_service import PubSubClient, XS_TYPE, XP_TYPE, TOPIC_TYPE, SUBSCRIBER_TYPE
from ion.services.coi import datastore

from ion.core.exception import ReceivedApplicationError, ReceivedError, ReceivedContainerError

from ion.core.object.gpb_wrapper import OOIObjectError

from ion.core import ioninit
from ion.core.object import object_utils, gpb_wrapper

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
           raise IngestionError('Could not get the datasource resource from the datastore')

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

        yield self._prepare_ingest(content)

        log.info('Created dataset details, Now setup subscriber...')

        ingest_data_topic = yield self._setup_ingestion_topic(content, headers.get('conv-id', "no-conv-id"))


        def _timeout():
            log.info("Timed out in op_perform_ingest")
            self._defer_ingest.errback(IngestionError('Time out in communication between the JAW and the Ingestion service', content.ResponseCodes.TIMEOUT))

        log.info('Setting up ingest timeout with value: %i' % content.ingest_service_timeout)
        self.timeoutcb = reactor.callLater(content.ingest_service_timeout, _timeout)

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

            # we have to notify that there is a failure, so get details and setup the dict to pass to notify_ingest.
            data_details = self.get_data_details(content)
            ingest_res={EM_ERROR:'Ingestion Failed: %s' % str(ex.message)}
            ingest_res.update(data_details)

            log.error("Error occured while waiting for ingestion to complete: %s" % str(ex.message))

            yield self._notify_ingest(ingest_res)

            # reraise - in the case of ApplicationError, will simply reply to the original sender
            # do NOT reraise in the case of a timeout on our side - JAW will timeout client-side
            if hasattr(ex, 'response_code') and ex.response_code == content.ResponseCodes.TIMEOUT:
                # ack the msg
                yield msg.ack()

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

            self.dataset.ResourceLifeCycleState = self.dataset.INACTIVE

        else:
            log.info("Ingest succeeded!")

            resources.append(self.dataset)

            # If the dataset / source is new 
            if self.dataset.ResourceLifeCycleState != self.dataset.ACTIVE:

                self.dataset.ResourceLifeCycleState = self.dataset.ACTIVE


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

        log.info('Adding 30 seconds to timeout')
        self.timeoutcb.delay(30)

        # notify JAW and others via event that we are still processing
        yield self._ingestion_processing_publisher.create_and_publish_event(origin=self.dataset.ResourceIdentity,
                                                                            dataset_id=self.dataset.ResourceIdentity,
                                                                            ingestion_process_id=self.id.full,
                                                                            conv_id=convid,
                                                                            processing_step="dataset")

        log.info(headers)

        if content.MessageType != CDM_DATASET_TYPE:
            raise IngestionError('Expected message type CDM Dataset Type, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)

        if self.dataset is None:
            raise IngestionError('Calling recv_dataset in an invalid state. No Dataset checked out to ingest.')

        if self.dataset.Repository.status is not self.dataset.Repository.UPTODATE:
            raise IngestionError('Calling recv_dataset in an invalid state. Dataset is already modified.')

        self.dataset.CreateUpdateBranch(content.MessageObject)

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

        log.info('Adding 30 seconds to timeout')
        self.timeoutcb.delay(30)
        # this is NOT rpc
        if content.MessageType != SUPPLEMENT_MSG_TYPE:
            raise IngestionError('Expected message type SupplementMessageType, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)
            
        if self.dataset is None:
            raise IngestionError('Calling recv_chunk in an invalid state. No Dataset checked out to ingest.')

        if self.dataset.ResourceLifeCycleState is not self.dataset.UPDATE:
            raise IngestionError('Calling recv_chunk in an invalid state. Dataset is not on an update branch!')

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

        log.info(headers)
        


        log.info(type(content))
        log.info(str(content.Message))

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


        log.debug('_merge_overlapping_supplement - Start')

        raise NotImplementedError('OVERWRITE Supplement updates are not yet supported')


    @defer.inlineCallbacks
    def _merge_fmrc_supplement(self):


        log.debug('_merge_overlapping_supplement - Start')

        raise NotImplementedError('FMRC Supplement updates are not yet supported')



    @defer.inlineCallbacks
    def _merge_overlapping_supplement(self):


        log.debug('_merge_overlapping_supplement - Start')

        # A little sanity check on entering recv_done...
        if len(self.dataset.Repository.branches) != 2:
            raise IngestionError('The dataset is in a bad state - there should be two branches in the repository state on entering recv_done.', 500)


        # Commit the current state of the supplement - ingest of new content is complete
        self.dataset.Repository.commit('Ingest received complete notification.')

        # The current branch on entering recv done is the supplement branch
        merge_branch = self.dataset.Repository.current_branch_key()

        # Merge it with the current state of the dataset in the datastore
        yield self.dataset.MergeWith(branchname=merge_branch, parent_branch='master')

        #Remove the head for the supplement - there is only one current state once the merge is complete!
        self.dataset.Repository.remove_branch(merge_branch)


        # Get the root group of the current state of the dataset
        root = self.dataset.root_group

        # Get the root group of the supplement we are merging
        merge_root = self.dataset.Merge[0].root_group

        log.info('Starting Find Dimension LooP')

        time_vars = self._find_time_var(merge_root)

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
        merge_agg_dim = dimension_order[0]

        log.info('Merge aggregation dimension name is: %s' % merge_agg_dim.name)


        supplement_length = merge_agg_dim.length

        result = {EM_TIMESTEPS:supplement_length}

        agg_offset = 0
        try:
            agg_dim = root.FindDimensionByName(merge_agg_dim.name)
            agg_offset = agg_dim.length
            log.info('Aggregation offset from current dataset: %d' % agg_offset)

        except OOIObjectError, oe:
            log.debug('No Dimension found in current dataset:' + str(oe))

        # Get the start time of the supplement
        try:
            string_time = merge_root.FindAttributeByName('ion_time_coverage_start')
            supplement_stime = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))

            string_time = merge_root.FindAttributeByName('ion_time_coverage_end')
            supplement_etime = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))

            result.update({EM_START_DATE:supplement_stime*1000,
                           EM_END_DATE:supplement_etime*1000})

        except OOIObjectError, oe:
            log.debug('No start time attribute found in dataset supplement!' + str(oe))
            raise IngestionError('No start time attribute found in dataset supplement!')
            # this is an error - the attribute must be present to determine how to append the data supplement time coordinate!


        # Get the end time of the current dataset
        try:
            string_time = root.FindAttributeByName('ion_time_coverage_end')
            current_etime = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))

            if current_etime == supplement_stime:
                agg_offset -= 1
                log.info('Aggregation offset decremented by one - supplement overlaps: %d' % agg_offset)

            elif current_etime > supplement_stime:

                string_time_ds_end = string_time.GetValue()
                string_time_sup_start = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(supplement_stime))
                raise IngestionError('Can not aggregate dataset supplements which overlap by more than one timestep.  Dataset end time: "%s"  Supplement start time: "%s"' % (string_time_ds_end, string_time_sup_start))

            else:
                log.info('Aggregation offset unchanged - supplement does not overlap.')

        except OOIObjectError, oe:
            log.debug(oe)
            log.info('Aggregation offset unchanged - dataset has no ion_time_coverage_end.')
            # This is not an error - it is a new dataset.

        ###
        ### Add the dimensions from the supplement to the current state if they are not already there
        ###
        merge_dims = {}
        for merge_dim in merge_root.dimensions:
            merge_dims[merge_dim.name] = merge_dim

        dims = {}
        for dim in root.dimensions:
            dims[dim.name] = dim

        if merge_dims.keys() != dims.keys():
            if len(dims) != 0:
                raise IngestionError('Can not ingest supplement with different dimensions than the dataset')
            else:
                for merge_dim in merge_root.dimensions:
                    dim_link = root.dimensions.add()
                    dim_link.SetLink(merge_dim)

        else:
            # We are appending an existing dataset - adjust the length of the aggregation dimension
            agg_dim = dims[merge_agg_dim.name]
            agg_dim.length = agg_offset + supplement_length
            log.info('Setting the aggregation dimension %s to %d' % (agg_dim.name, agg_dim.length))


        for merge_var in merge_root.variables:
            var_name = merge_var.name

            log.info('Merge Var Name: %s' % merge_var.name)


            try:
                var = root.FindVariableByName(var_name)
            except OOIObjectError, oe:
                log.debug(oe)
                log.info('Variable %s does not yet exist in the dataset!' % var_name)

                v_link = root.variables.add()
                v_link.SetLink(merge_var, ignore_copy_errors=True)

                log.info('Copied Variable %s into the dataset!' % var_name)
                continue # Go to next variable...


            if merge_agg_dim not in merge_var.shape:
                log.info('Nothing to merge on variable %s which does not share the aggregation dimension' % var_name)
                continue # Ignore this variable...


            # @TODO check attributes for variables which are not aggregated....


            for merge_ba in merge_var.content.bounded_arrays:
                ba = var.Repository.copy_object(merge_ba, deep_copy=False)

                ba.bounds[0].origin += agg_offset

                ba_link = var.content.bounded_arrays.add()
                ba_link.SetLink(ba)

            log.info('Merged Variable %s into the dataset!' % var_name)

            
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


        # @TODO Get the vertical positive 'direction!' Deal with attributes accordingly.



        try:
            merge_att = merge_root.FindAttributeByName('ion_geospatial_vertical_positive')
            merge_vertical_positive = merge_att.GetValue()
        except OOIObjectError, oe:
            log.debug(oe)
            merge_vertical_positive = None

        try:
            att = root.FindAttributeByName('ion_geospatial_vertical_positive')
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


        for merge_att in merge_root.attributes:

            att_name = merge_att.name
            try:


                log.info('Merging Attribute: %s' % att_name)

                if att_name == 'ion_time_coverage_start':
                    root.MergeAttLesser(att_name, merge_root)

                elif att_name == 'ion_time_coverage_end':
                    root.MergeAttGreater(att_name, merge_root)

                elif att_name == 'ion_geospatial_lat_min':
                    root.MergeAttLesser(att_name, merge_root)

                elif att_name == 'ion_geospatial_lat_max':
                    root.MergeAttGreater(att_name, merge_root)
                
                elif att_name == 'ion_geospatial_lon_min':
                    # @TODO Need a better method to merge these - determine the greater extent of a wrapped coordinate
                    root.MergeAttSrc(att_name, merge_root)

                elif att_name == 'ion_geospatial_lon_max':
                    # @TODO Need a better method to merge these - determine the greater extent of a wrapped coordinate
                    root.MergeAttSrc(att_name, merge_root)


                elif att_name == 'ion_geospatial_vertical_min':
                    
                    # Check vert min/max for NaN, either is NaN or missing, don't merge
                    min = False
                    max = False
                    if merge_root.HasAttribute('ion_geospatial_vertical_min'):
                        val = merge_root.FindAttributeByName('ion_geospatial_vertical_min').GetValue()
                        if not pu.isnan(val):
                            min = True
                    
                    # Only check for max if min is available (if either value is not available, we can't continue)
                    if min is not None:
                        if merge_root.HasAttribute('ion_geospatial_vertical_max'):
                            val = merge_root.FindAttributeByName('ion_geospatial_vertical_max').GetValue()
                            if not pu.isnan(val):
                                max = True
                    
                    
                    if min and max:
                        if vertical_positive == 'down':
                            root.MergeAttLesser(att_name, merge_root)
    
                        elif vertical_positive == 'up':
                            root.MergeAttGreater(att_name, merge_root)
    
                        else:
                            raise OOIObjectError('Invalid value for Vertical Positive but ion_geospatial_vertical_min is present')
                    else:
                        root_min = root.HasAttribute('ion_geospatial_vertical_min')
                        root_max = root.HasAttribute('ion_geospatial_vertical_max')
                        
                        # if root doesnt have min/max, add new attributes with default values...
                        if not root_min and not root_max:
                            root.AddAttribute('ion_geospatial_vertical_min', root.DataType.DOUBLE, float('nan'))
                            root.AddAttribute('ion_geospatial_vertical_max', root.DataType.DOUBLE, float('nan'))


                elif att_name == 'ion_geospatial_vertical_max':

                    # Check vert min/max for NaN, either is NaN or missing, don't merge
                    min = False
                    max = False
                    if merge_root.HasAttribute('ion_geospatial_vertical_min'):
                        val = merge_root.FindAttributeByName('ion_geospatial_vertical_min').GetValue()
                        if not pu.isnan(val):
                            min = True
                    
                    # Only check for max if min is available (if either value is not available, we can't continue)
                    if min is not None:
                        if merge_root.HasAttribute('ion_geospatial_vertical_max'):
                            val = merge_root.FindAttributeByName('ion_geospatial_vertical_max').GetValue()
                            if not pu.isnan(val):
                                max = True
                    
                    
                    if min and max:
                        if vertical_positive == 'down':
                            root.MergeAttGreater(att_name, merge_root)
    
                        elif vertical_positive == 'up':
                            root.MergeAttLesser(att_name, merge_root)
    
                        else:
                            raise OOIObjectError('Invalid value for Vertical Positive but ion_geospatial_vertical_max is present')
                    else:
                        root_min = root.HasAttribute('ion_geospatial_vertical_min')
                        root_max = root.HasAttribute('ion_geospatial_vertical_max')
                        
                        # if root doesnt have min/max, add new attributes with default values...
                        if not root_min and not root_max:
                            root.AddAttribute('ion_geospatial_vertical_min', root.DataType.DOUBLE, float('nan'))
                            root.AddAttribute('ion_geospatial_vertical_max', root.DataType.DOUBLE, float('nan'))
                        


                elif att_name == 'history':
                    # @TODO is this the correct treatment for history?
                    root.MergeAttDstOver(att_name, merge_root)

                else:
                    root.MergeAttSrc(att_name, merge_root)

            except OOIObjectError, oe:

                log.exception('Attribute merger failed for global attribute: %s' % att_name)
                result[EM_ERROR] = 'Error during ingestion of global attributes'
            
            except ValueError, ex:
                
                log.exception('Attribute merger failed for global attribute "%s".  Cause: %s' % (att_name, str(ex)))


        log.debug('_merge_overlapping_supplement - Complete')

        defer.returnValue(result)

    def _find_time_var(self, group):

        time_vars = []

        for var in group.variables:

            #Parse the atts - try to short cut logic to identify time...
            for att in var.attributes:

                if att.name == 'standard_name' and att.GetValue() == 'time':
                    log.debug('Found standard name "time" in variable named: %s' % var.name)

                    time_vars.append(var)

                elif att.name == 'units' and att.GetValue().find(' since '):
                    log.debug('Found units att with "since" in variable named: %s' % var.name)

                    time_vars.append(var)

        return time_vars



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
