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
from ion.services.dm.distribution.events import DatasetSupplementAddedEventPublisher, DatasourceUnavailableEventPublisher
import ion.util.ionlog
from twisted.internet import defer, reactor
from twisted.python import reflect

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.dm.distribution.publisher_subscriber import Subscriber, PublisherFactory

from ion.services.dm.ingestion import cdm_attribute_methods

from ion.core.exception import ApplicationError

# For testing - used in the client
from ion.services.dm.distribution.pubsub_service import PubSubClient, XS_TYPE, XP_TYPE, TOPIC_TYPE, SUBSCRIBER_TYPE
from ion.services.coi import datastore

from ion.core.exception import ReceivedApplicationError, ReceivedError, ReceivedContainerError

from ion.core.object.gpb_wrapper import OOIObjectError

from ion.core import ioninit
from ion.core.object import object_utils

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

        log.info('IngestionService.__init__()')

    @defer.inlineCallbacks
    def slc_activate(self):

        log.info('Activation - Start')

        pub_factory = PublisherFactory(process=self)

        self._notify_ingest_publisher = yield pub_factory.build(publisher_type=DatasetSupplementAddedEventPublisher)

        self._notify_unavailable_publisher = yield pub_factory.build(publisher_type=DatasourceUnavailableEventPublisher)

        log.info('Activation - Complete')


    @defer.inlineCallbacks
    def op_create_dataset_topics(self, content, headers, msg):
        """
        Creates ingestion and notification topics that can be used to publish ingestion
        data and notifications about ingestion.
        """

        log.info('op_create_dataset_topics - Start')

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

        log.info('op_create_dataset_topics - Complete')



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
    def _prepare_ingest(self, content):
        """
        Factor out the preparation for ingestion so that we can unit test functionality
        """

        log.debug('_prepare_ingest - Start')

        # Get the current state of the dataset:
        self.dataset = yield self.rc.get_instance(content.dataset_id, excluded_types=[CDM_BOUNDED_ARRAY_TYPE])

        log.info('Got dataset resource')

        # Get the bounded arrays but not the ndarrays
        ba_links = []
        for var in self.dataset.root_group.variables:
            var_links = var.content.bounded_arrays.GetLinks()
            ba_links.extend(var_links)

        yield self.dataset.Repository.fetch_links(ba_links)

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

        log.debug('_prepare_ingest - Complete')

        defer.returnValue(data_details)

    @defer.inlineCallbacks
    def _setup_ingestion_topic(self, content):

        log.debug('_setup_ingestion_topic - Start')

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

        log.debug('_setup_ingestion_topic - Complete')

        defer.returnValue(ingest_data_topic)


    @defer.inlineCallbacks
    def op_ingest(self, content, headers, msg):
        """
        Start the ingestion process by setting up necessary
        @TODO NO MORE MAGNET.TOPIC
        """
        log.info('op_ingest - Start')

        if content.MessageType != PERFORM_INGEST_MSG_TYPE:
            raise IngestionError('Expected message type PerfromIngestRequest, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)

        data_details = yield self._prepare_ingest(content)

        log.info('Created dataset details, Now setup subscriber...')

        ingest_data_topic = yield self._setup_ingestion_topic(content)


        def _timeout():
            # trigger execution to continue below with a False result
            log.info("Timed out in op_perform_ingest")

            result = {'status'      :'Internal Timeout',
                      'status_body' :'Time out in communication between the JAW and the Ingestion service'}
            self._defer_ingest.callback(result)

        log.info('Setting up ingest timeout with value: %i' % content.ingest_service_timeout)
        timeoutcb = reactor.callLater(content.ingest_service_timeout, _timeout)

        log.info(
            'Notifying caller that ingest is ready by invoking op_ingest_ready() using routing key: "%s"' % content.reply_to)
        irmsg = yield self.mc.create_instance(INGESTION_READY_TYPE)
        irmsg.xp_name = "magnet.topic"
        irmsg.publish_topic = ingest_data_topic

        self.send(content.reply_to, operation='ingest_ready', content=irmsg)

        log.info("Yielding in op_perform_ingest for receive loop to complete")
        ingest_res = yield self._defer_ingest    # wait for other commands to finish the actual ingestion

        # common cleanup

        # we succeeded, cancel the timeout
        timeoutcb.cancel()

        # reset ingestion deferred so we can use it again
        self._defer_ingest = defer.Deferred()

        # remove subscriber, deactivate it
        self._registered_life_cycle_objects.remove(self._subscriber)
        yield self._subscriber.terminate()
        self._subscriber = None

        if ingest_res:
            log.debug("Ingest succeeded, respond to original request")

            ingest_res.update(data_details)

            self.rc.put_instance(self.dataset)

            # send notification we performed an ingest
            yield self._notify_ingest(ingest_res)


            # now reply ok to the original message
            yield self.reply_ok(msg)
        else:
            log.debug("Ingest failed, error back to original request")
            raise IngestionError("Ingestion failed", content.ResponseCodes.INTERNAL_SERVER_ERROR)

        log.info('op_ingest - Complete')



    @defer.inlineCallbacks
    def _notify_ingest(self, ingest_res):
        """
        Generate a notification/event that an ingest succeeded.
        """

        log.debug('_notify_ingest - Start')


        if ingest_res.has_key(EM_ERROR):
            # Report an error with the data source
            datasource_id = ingest_res[EM_DATA_SOURCE]
            yield self._notify_unavailable_publisher.create_and_publish_event(origin=datasource_id, **ingest_res)
        else:
            # Report a successful update to the dataset
            dataset_id = ingest_res[EM_DATASET]
            yield self._notify_ingest_publisher.create_and_publish_event(origin=dataset_id, **ingest_res)

        log.debug('_notify_ingest - Complete')


    @defer.inlineCallbacks
    def op_recv_dataset(self, content, headers, msg):
        log.info("op_recv_dataset(%s)" % type(content))
        # this is NOT rpc

        log.info('op_recv_dataset - Start')

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

                        if not ba.IsFieldSet('ndarray'):
                            del content.bounded_arrays[i]

                            continue
                        else:
                            i += 1
            else:
                var.content = resource_instance.CreateObject(array_structure_type)

        yield msg.ack()

        log.info('op_recv_dataset - Complete')


    @defer.inlineCallbacks
    def op_recv_chunk(self, content, headers, msg):

        log.info('op_recv_chunk - Start')

        # this is NOT rpc
        if content.MessageType != SUPPLEMENT_MSG_TYPE:
            raise IngestionError('Expected message type SupplementMessageType, received %s'
                                 % str(content), content.ResponseCodes.BAD_REQUEST)
            
        if self.dataset is None:
            raise IngestionError('Calling recv_chunk in an invalid state. No Dataset checked out to ingest.')

        if self.dataset.ResourceLifeCycleState is not self.dataset.UPDATE:
            raise IngestionError('Calling recv_chunk in an invalid state. Dataset is not on an update branch!')

        if content.dataset_id != self.dataset.ResourceIdentity:
            raise IngestionError('Calling recv_chunk with a dataset that does not match the received chunk!.')


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

        log.info('op_recv_chunk - Complete')


    @defer.inlineCallbacks
    def op_recv_done(self, content, headers, msg):
        """
        @TODO deal with FMRC datasets and supplements
        """

        log.info('op_recv_done - Start')

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

            self._defer_ingest.callback(result)

        else:

            #@TODO ask dave for help here - how can I chain these callbacks?
            yield self._merge_supplement()


        # this is NOT rpc
        yield msg.ack()

        log.info('op_recv_done - Complete')


    @defer.inlineCallbacks
    def _merge_supplement(self):


        log.debug('_merge_supplement - Start')

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

        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'

        #print root.PPrint()

        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM'


        # Get the root group of the supplement we are merging
        merge_root = self.dataset.Merge[0].root_group

        # Determine the inner most dimension on which we are aggregating
        dimension_order = []
        for merge_var in merge_root.variables:

            # Add each dimension in reverse order so that the inside dimension is always in front... to determine the time aggregation dimension
            for merge_dim in reversed(merge_var.shape):

                if merge_dim not in dimension_order:
                    dimension_order.insert(0, merge_dim)

        #print 'FINAL DIMENSION ORDER:'
        #print [ dim.name for dim in dimension_order]

        # This is the inner most!
        merge_agg_dim = dimension_order[0]

        supplement_length = merge_agg_dim.length

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

        except OOIObjectError, oe:
            log.debug('No start time attribute found in dataset supplement!' + str(oe))
            raise IngestionError('No start time attribute found in dataset supplement!')


        # Get the end time of the current dataset
        try:
            string_time = root.FindAttributeByName('ion_time_coverage_end')
            current_etime = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))

            if current_etime == supplement_stime:
                agg_offset -= 1
                log.info('Aggregation offset decremented by one - supplement overlaps: %d' % agg_offset)

            elif current_etime > supplement_stime:

                raise IngestionError('Can not aggregate dataset supplements which overlap by more than one timestep.')

            else:
                log.info('Aggregation offset unchanged - supplement does not overlap.')

        except OOIObjectError, oe:
            log.debug(oe)
            log.info('Aggregation offset unchanged - dataset has no ion_time_coverage_end.')



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
            agg_dim.length += agg_offset




        for merge_var in merge_root.variables:
            var_name = merge_var.name

            try:
                var = root.FindVariableByName(var_name)
            except OOIObjectError, oe:
                log.debug(oe)
                log.info('Variable %s does not yet exist in the dataset!' % var_name)

                v_link = root.variables.add()
                v_link.SetLink(merge_var)

                log.info('Copied Variable %s into the dataset!' % var_name)
                continue # Go to next variable...


            if merge_agg_dim not in merge_var.shape:
                log.info('Nothing to merge on variable %s which does not share the aggregation dimension' % var_name)
                continue # Ignore this variable...


            # @TODO check attributes for variables which are not aggregated....



            #print 'MERGEING VAR %s' % var_name
            #print var.content.PPrint()

            for merge_ba in merge_var.content.bounded_arrays:
                ba = var.Repository.copy_object(merge_ba, deep_copy=False)

                ba.bounds[0].origin += agg_offset

                ba_link = var.content.bounded_arrays.add()
                ba_link.SetLink(ba)

            log.info('Merged Variable %s into the dataset!' % var_name)

            #print 'MERGED VAR....'
            #print var.content.PPrint()
            #print 'MERGEING Complete %s' % var_name

            merge_att_ids = set()
            for merge_att in merge_var.attributes:
                merge_att_ids.add(merge_att.MyId)

            att_ids = set()
            for att in var.attributes:
                att_ids.add(att.MyId)

            if att_ids != merge_att_ids:
                raise ImportError('Variable %s attributes are not the same in the supplement!' % var_name)


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

            if att_name == 'ion_time_coverage_start':
                cdm_attribute_methods.MergeAttLesser(root, att_name, merge_root)

            elif att_name == 'ion_time_coverage_end':
                cdm_attribute_methods.MergeAttGreater(root, att_name, merge_root)

            elif att_name == 'ion_geospatial_lat_min':
                cdm_attribute_methods.MergeAttLesser(root, att_name, merge_root)

            elif att_name == 'ion_geospatial_lat_max':
                cdm_attribute_methods.MergeAttGreater(root, att_name, merge_root)

            elif att_name == 'ion_geospatial_lon_min':
                # @TODO Need a better method to merge these - determine the greater extent of a wrapped coordinate
                cdm_attribute_methods.MergeAttSrc(root, att_name, merge_root)

            elif att_name == 'ion_geospatial_lon_max':
                # @TODO Need a better method to merge these - determine the greater extent of a wrapped coordinate
                cdm_attribute_methods.MergeAttSrc(root, att_name, merge_root)


            elif att_name == 'ion_geospatial_vertical_min':

                if vertical_positive == 'down':
                    cdm_attribute_methods.MergeAttLesser(root, att_name, merge_root)

                elif vertical_positive == 'up':
                    cdm_attribute_methods.MergeAttGreater(root, att_name, merge_root)

                else:
                    raise OOIObjectError('Invalid value for Vertical Positive')


            elif att_name == 'ion_geospatial_vertical_max':

                if vertical_positive == 'down':
                    cdm_attribute_methods.MergeAttGreater(root, att_name, merge_root)

                elif vertical_positive == 'up':
                    cdm_attribute_methods.MergeAttLesser(root, att_name, merge_root)

                else:
                    raise OOIObjectError('Invalid value for Vertical Positive')


            elif att_name == 'history':
                # @TODO is this the correct treatment for history?
                cdm_attribute_methods.MergeAttDstOver(root, att_name, merge_root)

            else:
                cdm_attribute_methods.MergeAttSrc(root, att_name, merge_root)



        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'

        #print root.PPrint()

        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'
        #print 'LMDLDMDLMDLMDLDMDLMDLDMDLMDLDMDLM2222'


        result = {EM_START_DATE:supplement_stime*1000,
                  EM_END_DATE:supplement_etime*1000,
                  EM_TIMESTEPS:supplement_length}


        log.debug('_merge_supplement - Complete')

        # trigger the op_perform_ingest to complete!
        self._defer_ingest.callback(result)


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
        log.debug('-[]- Entered IngestionClient.perform_ingest()')
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()

        ingest_service_timeout = msg.ingest_service_timeout

        # Invoke [op_]() on the target service 'dispatcher_svc' via RPC
        log.info("@@@--->>> Sending 'perform_ingest' RPC message to ingestion service")
        (content, headers, msg) = yield self.rpc_send('ingest', msg, timeout=ingest_service_timeout + 30)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def create_dataset_topics(self, msg):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('create_dataset_topics', msg)
        defer.returnValue(content)

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


# Spawn of the process using the module name
factory = ProcessFactory(IngestionService)
