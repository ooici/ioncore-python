#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/test/test_ingestion.py
@author David Stuebe
@brief test for eoi ingestion demo
"""
from ion.core.exception import ReceivedApplicationError
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.dm.distribution.publisher_subscriber import Subscriber, Publisher

import logging, time, calendar
import ion.util.ionlog
from ion.util.iontime import IonTime

log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor
from twisted.trial import unittest
import random

from ion.core import ioninit
from ion.util import procutils as pu
from ion.services.coi.datastore_bootstrap.ion_preload_config import PRELOAD_CFG, ION_DATASETS_CFG, SAMPLE_PROFILE_DATASET_ID, SAMPLE_PROFILE_DATA_SOURCE_ID, TYPE_CFG, NAME_CFG, DESCRIPTION_CFG, CONTENT_CFG, CONTENT_ARGS_CFG, ID_CFG

from ion.services.dm.distribution.events import DatasourceUnavailableEventSubscriber, DatasetSupplementAddedEventSubscriber, DATASET_STREAMING_EVENT_ID, get_events_exchange_point

from ion.core.process import process
from ion.services.dm.ingestion.ingestion import IngestionClient, IngestionError, SUPPLEMENT_MSG_TYPE, CDM_DATASET_TYPE, DAQ_COMPLETE_MSG_TYPE, PERFORM_INGEST_MSG_TYPE, CREATE_DATASET_TOPICS_MSG_TYPE, EM_URL, EM_ERROR, EM_TITLE, EM_DATASET, EM_END_DATE, EM_START_DATE, EM_TIMESTEPS, EM_DATA_SOURCE, CDM_BOUNDED_ARRAY_TYPE 
from ion.test.iontest import IonTestCase

from ion.services.coi.datastore_bootstrap.dataset_bootstrap import bootstrap_profile_dataset, BOUNDED_ARRAY_TYPE, FLOAT32ARRAY_TYPE, bootstrap_byte_array_dataset

from ion.core.object.object_utils import create_type_identifier, ARRAY_STRUCTURE_TYPE
from ion.core.messaging.message_client import MessageInstance

DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
DATASOURCE_TYPE = create_type_identifier(object_id=4503, version=1)
GROUP_TYPE = create_type_identifier(object_id=10020, version=1)


CONF = ioninit.config(__name__)


class AggregationRule():
    OVERLAP   = 1
    OVERWRITE = 2
    FMRC      = 3

def create_delayed_call(timeoutval=None):
    timeoutval = timeoutval or 10000
    def _timeout():
        # do nothing
        pass
    dc = reactor.callLater(timeoutval, _timeout)
    dc.ingest_service_timeout = timeoutval
    return dc

class IngestionTest(IonTestCase):
    """
    Testing service operations of the ingestion service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {   'name':'ds1',
                'module':'ion.services.coi.datastore',
                'class':'DataStoreService',
                'spawnargs':
                        {PRELOAD_CFG:
                                 {ION_DATASETS_CFG:True}
                        }
            },

            {
                'name':'resource_registry1',
                'module':'ion.services.coi.resource_registry.resource_registry',
                'class':'ResourceRegistryService',
                    'spawnargs':{'datastore_service':'datastore'}
            },

            {
                'name':'exchange_management',
                'module':'ion.services.coi.exchange.exchange_management',
                'class':'ExchangeManagementService',
            },

            {
                'name':'association_service',
                'module':'ion.services.dm.inventory.association_service',
                'class':'AssociationService'
            },
            {
                'name':'pubsub_service',
                'module':'ion.services.dm.distribution.pubsub_service',
                'class':'PubSubService'
            },

            {   'name':'ingestion1',
                'module':'ion.services.dm.ingestion.ingestion',
                'class':'IngestionService'
            },

            ]

        # ADD PUBSUB AND EMS

        self.sup = yield self._spawn_processes(services)

        self.proc = process.Process(spawnargs={'proc-name':'test_ingestion_proc'})
        yield self.proc.spawn()

        self._ic = IngestionClient(proc=self.proc)

        ingestion1 = yield self.sup.get_child_id('ingestion1')
        log.debug('Process ID:' + str(ingestion1))
        self.ingest= self._get_procinstance(ingestion1)

        ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(ds1))
        self.datastore= self._get_procinstance(ds1)

        self.rc = ResourceClient(proc=self.proc)


    class fake_msg(object):

        def ack(self):
            return True


    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_create_dataset_topics(self):
        """
        """

        msg = yield self.proc.message_client.create_instance(CREATE_DATASET_TOPICS_MSG_TYPE)
        msg.dataset_id = 'ABC'
        result = yield self._ic.create_dataset_topics(msg)
        self.failUnlessEquals(result.MessageResponseCode, result.ResponseCodes.OK)

        # let's try again, monkeypatching the config object alltogether
        class FakeConf(object):
            def getValue(self, *args, **kwargs):
                return True
        fakeconf = FakeConf()

        import ion.services.dm.ingestion.ingestion as im
        oldconf = im.CONF
        im.CONF = fakeconf

        msg = yield self.proc.message_client.create_instance(CREATE_DATASET_TOPICS_MSG_TYPE)
        msg.dataset_id = 'DEF'
        result = yield self._ic.create_dataset_topics(msg)
        self.failUnlessEquals(result.MessageResponseCode, result.ResponseCodes.OK)
        self.failUnless(self.ingest._xs_id is not None)
        self.failUnless(self.ingest._xp_id is not None)

        im.CONF = oldconf

    @defer.inlineCallbacks
    def test_prepare_ingest(self):
        class FakeContent(object):
            dataset_id = "1"
            datasource_id = "2"

        ex = yield self.failUnlessFailure(self.ingest._prepare_ingest(FakeContent()), IngestionError)
        self.failUnlessIn("dataset resource", str(ex))

        fc = FakeContent()
        fc.dataset_id = SAMPLE_PROFILE_DATASET_ID

        ex = yield self.failUnlessFailure(self.ingest._prepare_ingest(fc), IngestionError)
        self.failUnlessIn("datasource resource", str(ex))

        # make it through ok
        fc.datasource_id = SAMPLE_PROFILE_DATA_SOURCE_ID

        yield self.ingest._prepare_ingest(fc)

    @defer.inlineCallbacks
    def test_setup_ingestion_topic(self):
        class FakeContent(object):
            dataset_id = "1"
            datasource_id = "2"

        bk = yield self.ingest._setup_ingestion_topic(FakeContent(), "convid")
        self.failUnless(bk is not None)

        # monkey patch validity checker!
        def invalid(*args):
            return False

        self.ingest._ingest_data_topic_valid = invalid

        # still works! just prints a log error
        bk = yield self.ingest._setup_ingestion_topic(FakeContent(), "confid")
        self.failUnless(bk is not None)

    @defer.inlineCallbacks
    def test_handle_ingestion_msg(self):
        class FakeMsg(object):
            def __init__(self):
                self._state = "NONE"

            def ack(self):
                self._state = "SELFACKED"

        # monkeypatch ingestion's _recv methods
        self.recv_dataset_count = 0
        self.recv_chunk_count = 0
        self.recv_done_count = 0

        def fake_recv_dataset(*args):
            self.recv_dataset_count += 1
            d = defer.Deferred()
            reactor.callLater(0, d.callback, self.recv_dataset_count)
            return d

        def fake_recv_chunk(*args):
            self.recv_chunk_count += 1
            d = defer.Deferred()
            reactor.callLater(0, d.callback, self.recv_chunk_count)
            return d

        def fake_recv_done(*args):
            self.recv_done_count += 1
            d = defer.Deferred()
            reactor.callLater(0, d.callback, self.recv_done_count)
            return d

        self.ingest._ingest_op_recv_dataset = fake_recv_dataset
        self.ingest._ingest_op_recv_chunk = fake_recv_chunk
        self.ingest._ingest_op_recv_done = fake_recv_done

        # setup over, test ingestion terminating path (first check)
        self.ingest._ingestion_terminating = True
        fm = FakeMsg()
        meh = yield self.ingest._handle_ingestion_msg({}, fm, "confid")
        self.failUnlessEquals(meh, False)
        self.failUnlessEquals(fm._state, "ACKED")

        self.ingest._ingestion_terminating = False

        # now test each op - first, no op specified - error
        yield self.ingest._handle_ingestion_msg({}, FakeMsg(), "convid")
        self.failUnlessEquals(self.ingest._ingestion_terminating, True)

        # must clear out deferred or trial will complain
        try:
            yield self.ingest._defer_ingest
        except Exception, ex:
            self.failUnlessIn("Unknown operation", str(ex))

        self.ingest._ingestion_terminating = False
        self.ingest._defer_ingest = defer.Deferred()

        # now recv dataset
        yield self.ingest._handle_ingestion_msg({'op':'recv_dataset'}, FakeMsg(), "convid")
        self.failUnlessEquals(self.recv_dataset_count, 1)

        # now recv chunk
        yield self.ingest._handle_ingestion_msg({'op':'recv_chunk'}, FakeMsg(), "convid")
        self.failUnlessEquals(self.recv_chunk_count, 1)

        # now recv done
        yield self.ingest._handle_ingestion_msg({'op':'recv_done'}, FakeMsg(), "convid")
        self.failUnlessEquals(self.recv_done_count, 1)

        # now patch one method to error and make sure exception path is taken too
        def recv_ex(*args):
            raise Exception("i am a robot")

        self.ingest._ingest_op_recv_dataset = recv_ex
        self.ingest._defer_ingest = defer.Deferred()

        fm = FakeMsg()
        yield self.ingest._handle_ingestion_msg({'op':'recv_dataset'}, fm, "convid")

        self.failUnlessEquals(self.ingest._ingestion_terminating, True)
        self.failUnlessEquals(fm._state, "SELFACKED")
        self.failUnless(self.ingest._defer_ingest.called)

        # must clear out deferred or trial will complain
        try:
            yield self.ingest._defer_ingest
        except Exception, ex:
            self.failUnlessIn("robot", str(ex))

    @defer.inlineCallbacks
    def test_recv_dataset(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """
        #print '\n\n\n Starting Test \n\n\n\n'
        # Reach into the ingestion service and fake the receipt of a perform ingest method - so we can test recv_dataset

        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID
        content.datasource_id = SAMPLE_PROFILE_DATA_SOURCE_ID



        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = create_delayed_call()

        #print '\n\n\n Got Dataset in Ingest \n\n\n\n'

        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        yield bootstrap_profile_dataset(cdm_dset_msg, supplement_number=1, random_initialization=True)

        #print '\n\n\n Filled out message with a dataset \n\n\n\n'

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())

        # ==========
        # Can't use messaging and client because the send returns before the op is complete so the result is untestable.
        #yield self._ic.send_dataset(SAMPLE_PROFILE_DATASET_ID,cdm_dset_msg)
        #yield pu.asleep(1)
        # ==========

        self.assertEqual(self.ingest.dataset.ResourceLifeCycleState, self.ingest.dataset.UPDATE)



    @defer.inlineCallbacks
    def test_recv_chunk(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        #print '\n\n\n Starting Test \n\n\n\n'
        # Reach into the ingestion service and fake the receipt of a perform ingest method - so we can test recv_dataset

        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID
        content.datasource_id = SAMPLE_PROFILE_DATA_SOURCE_ID

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = create_delayed_call()

        self.ingest.dataset.CreateUpdateBranch()

        #print '\n\n\n Got Dataset in Ingest \n\n\n\n'

        # Pick a few variables to 'update'
        var_list = ['time', 'depth', 'lat', 'lon', 'salinity']

        for var in var_list:

            yield self.create_and_test_variable_chunk(var)


    @defer.inlineCallbacks
    def create_and_test_variable_chunk(self, var_name):

        group = self.ingest.dataset.root_group
        var = group.FindVariableByName(var_name)
        starting_bounded_arrays  = var.content.bounded_arrays[:]

        supplement_msg = yield self.ingest.mc.create_instance(SUPPLEMENT_MSG_TYPE)
        supplement_msg.dataset_id = SAMPLE_PROFILE_DATASET_ID
        supplement_msg.variable_name = var_name

        self.create_chunk(supplement_msg)

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_chunk(supplement_msg, '', self.fake_msg())

        updated_bounded_arrays = var.content.bounded_arrays[:]

        # This is all we really need to do - make sure that the bounded array has been added.
        self.assertEqual(len(updated_bounded_arrays), len(starting_bounded_arrays)+1)

        # The bounded array but not the ndarray should be in the ingestion service dataset
        self.assertIn(supplement_msg.bounded_array.MyId, self.ingest.dataset.Repository.index_hash)
        self.assertNotIn(supplement_msg.bounded_array.ndarray.MyId, self.ingest.dataset.Repository.index_hash)

        # The datastore should now have this ndarray
        self.failUnless(self.datastore.b_store.has_key(supplement_msg.bounded_array.ndarray.MyId))


    def create_chunk(self, supplement_msg):
        """
        This method is specialized to create bounded arrays for the Sample profile dataset.
        """



        supplement_msg.bounded_array = supplement_msg.CreateObject(BOUNDED_ARRAY_TYPE)
        supplement_msg.bounded_array.ndarray = supplement_msg.CreateObject(FLOAT32ARRAY_TYPE)

        if supplement_msg.variable_name == 'time':

            tsteps = 3
            tstart = 1280106120
            delt = 3600
            supplement_msg.bounded_array.ndarray.value.extend([tstart + delt*n for n in range(tsteps)])

            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[0].origin = 0
            supplement_msg.bounded_array.bounds[0].size = tsteps

        elif supplement_msg.variable_name == 'depth':
            supplement_msg.bounded_array.ndarray.value.extend([0.0, 0.1, 0.2])
            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[0].origin = 0
            supplement_msg.bounded_array.bounds[0].size = 3

        elif supplement_msg.variable_name == 'salinity':
            supplement_msg.bounded_array.ndarray.value.extend([29.84, 29.76, 29.87, 30.16, 30.55, 30.87])
            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[0].origin = 0
            supplement_msg.bounded_array.bounds[0].size = 2
            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[1].origin = 0
            supplement_msg.bounded_array.bounds[1].size = 3


        supplement_msg.Repository.commit('Commit before fake send...')


    @defer.inlineCallbacks
    def test_recv_done(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        # Receive a dataset to get setup...
        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID
        content.datasource_id = SAMPLE_PROFILE_DATA_SOURCE_ID

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = create_delayed_call()

        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        yield bootstrap_profile_dataset(cdm_dset_msg, supplement_number=1, random_initialization=True)

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())


        complete_msg = yield self.ingest.mc.create_instance(DAQ_COMPLETE_MSG_TYPE)

        complete_msg.status = complete_msg.StatusCode.OK
        self.ingest._ingest_op_recv_done(complete_msg, '', self.fake_msg())

        result = yield self.ingest._defer_ingest
        self.assertNotIn(EM_ERROR, result)



    @defer.inlineCallbacks
    def test_recv_error(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        # Receive a dataset to get setup...
        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = SAMPLE_PROFILE_DATASET_ID
        content.datasource_id = SAMPLE_PROFILE_DATA_SOURCE_ID

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = create_delayed_call()

        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        yield bootstrap_profile_dataset(cdm_dset_msg, supplement_number=1, random_initialization=True)

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())


        complete_msg = yield self.ingest.mc.create_instance(DAQ_COMPLETE_MSG_TYPE)

        complete_msg.status = complete_msg.StatusCode.EXTERNAL_SERVER_UNAVAILABLE
        self.ingest._ingest_op_recv_done(complete_msg, '', self.fake_msg())

        result = yield self.ingest._defer_ingest

        self.assertIn(EM_ERROR, result)




    @defer.inlineCallbacks
    def test_merge_overlap(self):
        """
        This is a test method for the merge dataset operation (with overlaps) of the ingestion service
        where the supplement being merged is sent as a whole dataset
        """
        yield self._test_merge_overlap(False)


    @defer.inlineCallbacks
    def test_merge_overlap_chunks(self):
        """
        This is a test method for the merge dataset operation (with overlaps) of the ingestion service
        where the supplement being merged is sent in chunks
        """
        yield self._test_merge_overlap(True)
    
    
    @defer.inlineCallbacks
    def _test_merge_overlap(self, send_as_chunks):

        # Step 1: Ingest and merge a supplement
        #--------------------------------------
        log.debug('test_merge_overlap(): Merging first supplement (no overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, send_as_chunks=send_as_chunks, supplement_number=1)


        # Test the data in the datastore
        dataset = yield self.ingest.rc.get_instance(dataset_id, excluded_types=[])
        root    = dataset.root_group
        tvar    = root.FindVariableByName('time')
        
        self.assertEqual(len(tvar.content.bounded_arrays), 2)
        log.debug('test_merge_overlap(): Bounded array count: 2')

        
        # Make sure there are no overlaps for the first supplement
        ba_vals  = []
        for ba in tvar.content.bounded_arrays:
            value = ba.ndarray.value
            self.assertNotIn(value, ba_vals)
            ba_vals.extend(value)
        log.debug("test_merge_overlap(): Values for the 'time' variable: %s" % ba_vals)


        
        # Step 2: Now try ingesting a supplement which overlaps
        #------------------------------------------------------
        log.debug('test_merge_overlap(): Merging second supplement (3 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, send_as_chunks=send_as_chunks, supplement_number=2, supplement_overlap_count=3)
        
        
        # Test the data in the datastore
        dataset = yield self.ingest.rc.get_instance(dataset_id, excluded_types=[])
        root = dataset.root_group
        tvar = root.FindVariableByName('time')
        
        self.assertEqual(len(tvar.content.bounded_arrays), 3)
        log.debug('test_merge_overlap(): Bounded array count: 3')
        
        
        # Verify that the new data is present and there are exactly 3 overlaps
        prev_values = []
        prev_values.extend(tvar.content.bounded_arrays[0].ndarray.value)
        prev_values.extend(tvar.content.bounded_arrays[1].ndarray.value)
        self.assertEqual(len(prev_values), 4)
        
        next_values = []
        next_values.extend(tvar.content.bounded_arrays[2].ndarray.value)
        self.assertEqual(len(next_values), 5)
        
        self.assertIn(next_values[0], prev_values)          # Overlapping data
        self.assertIn(next_values[1], prev_values)          # Overlapping data
        self.assertIn(next_values[2], prev_values)          # Overlapping data
        self.assertNotIn(next_values[3], prev_values)       # New data
        self.assertNotIn(next_values[4], prev_values)       # New data
        
        
        # Verify the offsets for all the bounded arrays make sense
        self.assertEqual(len(tvar.content.bounded_arrays[0].bounds), 1)
        self.assertEqual(len(tvar.content.bounded_arrays[1].bounds), 1)
        self.assertEqual(len(tvar.content.bounded_arrays[2].bounds), 1)
        
        self.assertEqual(tvar.content.bounded_arrays[0].bounds[0].origin, 0)
        self.assertEqual(tvar.content.bounded_arrays[0].bounds[0].size, 2)

        self.assertEqual(tvar.content.bounded_arrays[1].bounds[0].origin, 2)
        self.assertEqual(tvar.content.bounded_arrays[1].bounds[0].size, 2)
        
        self.assertEqual(tvar.content.bounded_arrays[2].bounds[0].origin, 1)  # This origin would be 4, but this array contains a 3 overlapping values!
        self.assertEqual(tvar.content.bounded_arrays[2].bounds[0].size, 5)


        # Verify the number of reported values matches the number of actual values
        for ba in tvar.content.bounded_arrays:
            self.assertEqual(len(ba.ndarray.value), ba.bounds[0].size)
            
        
        # Verify that all values are monotonic and duplicate indices provided matching data
        value_list = []
        for i in range(len(tvar.content.bounded_arrays)):
            ba = tvar.content.bounded_arrays[i]
            for j in range(len(ba.ndarray.value)):
                val = ba.ndarray.value[j]
                key = ba.bounds[0].origin + j
                value_list.append((key, val))
        
        value_list.sort()
        last_key, last_val = value_list[0]
        for k,v in value_list[1:]:
            if k == last_key:
                self.assertEqual(v, last_val)  ## Fails if overlapping indices do not provide matching data values after merge
            elif k > last_key:
                self.assertTrue(v > last_val)  ## Fails if data in the 'time' variable is not monotonic after merge
            last_key = k
            last_val = v
            
            
        #  Verify that the ion_time_coverage_start and ion_time_coverage_end match the min/max values of the bounded_arrays
        start_time_s = root.FindAttributeByName('ion_time_coverage_start')
        start_time   = calendar.timegm(time.strptime(start_time_s.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
        
        end_time_s = root.FindAttributeByName('ion_time_coverage_end')
        end_time   = calendar.timegm(time.strptime(end_time_s.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
        
        self.assertEqual(long(start_time), tvar.content.bounded_arrays[0].ndarray.value[0])
        self.assertEqual(long(end_time),   tvar.content.bounded_arrays[-1].ndarray.value[-1])


        # @todo: Validate the arrangement of the salinity data mathematically?  (this should be possible because it is now created mathematically


    @defer.inlineCallbacks
    def test_merge_overlap_fail_total_overlap(self):
        
        # Step 1: Ingest and merge a supplement (supplement # 2 with 2 overlaps introduces 4 new values on top of the original dataset
        #--------------------------------------
        log.debug('test_merge_overlap(): Merging first supplement (no overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, supplement_number=2, supplement_overlap=2)

        excep = None
        try:
            # Now send a supplement which is encompassed by the dataset
            # In this case, overlap cannot properly append this data -- this should be done via overwrite, and so an exception is thrown.
            dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, supplement_number=1)
        except Exception, ex:
            excep = ex
            
        
        self.assertTrue(isinstance(excep, IngestionError), 'Ingestion is supposed to throw an IngestionError when the current dataset completely overlaps the dataset supplement')


    @defer.inlineCallbacks
    def test_merge_overlap_check_fetch_one_overlap(self):
        """
        This is a test method for the merge dataset operation (via overlap) of the ingestion service.
        This runs through an overlap test without first fetching the blobs for the dataset to ensure
        that the merge methods will fetch blobs when necessary
        """

        # Step 1: Ingest and merge a supplement which provides overlapping indices
        #-------------------------------------------------------------------------
        log.info('test_merge_overlap_check_fetch(): Merging first supplement (1 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, supplement_number=1, supplement_overlap_count=1)
        log.info('test_merge_overlap_check_fetch(): Merging second supplement (4 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, supplement_number=2, supplement_overlap_count=4)


    @defer.inlineCallbacks
    def test_merge_overlap_check_fetch_two_overlap(self):
        """
        This is a test method for the merge dataset operation (via overlap) of the ingestion service.
        This runs through an overlap test without first fetching the blobs for the dataset to ensure
        that the merge methods will fetch blobs when necessary
        
        When the supplement overlaps by more than one timestep a different code path is taken -- this test is for code coverage
        """

        # Step 1: Ingest and merge a supplement which provides overlapping indices
        #-------------------------------------------------------------------------
        log.info('test_merge_overlap_check_fetch(): Merging first supplement (2 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, supplement_number=1, supplement_overlap_count=2)
        log.info('test_merge_overlap_check_fetch(): Merging second supplement (4 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERLAP, supplement_number=2, supplement_overlap_count=4)
        
        
    @defer.inlineCallbacks
    def test_merge_overwrite(self):
        """
        This is a test method for the merge dataset operation (via overwrite) of the ingestion service
        where the supplement being merged is sent as a whole dataset
        """
        yield self._test_merge_overwrite(send_as_chunks=False)

    
    @defer.inlineCallbacks
    def test_merge_overwrite_chunks(self):
        """
        This is a test method for the merge dataset operation (via overwrite) of the ingestion service
        where the supplement being merged is sent in chunks
        """
        yield self._test_merge_overwrite(send_as_chunks=True)
    
    
    @defer.inlineCallbacks
    def _test_merge_overwrite(self, send_as_chunks):

        # Step 1: Ingest and merge a supplement
        #--------------------------------------
        log.info('test_merge_overwrite(): Merging first supplement (no overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=send_as_chunks, supplement_number=1)


        # Test the data in the datastore
        dataset = yield self.ingest.rc.get_instance(dataset_id, excluded_types=[])
        root    = dataset.root_group
        tvar    = root.FindVariableByName('time')
        
        self.assertEqual(len(tvar.content.bounded_arrays), 2)
        log.debug('test_merge_overwrite(): Bounded array count: 2')
        
        
        # Make sure there are no overlaps for the first supplement
        ba_vals  = []
        for ba in tvar.content.bounded_arrays:
            value = ba.ndarray.value
            self.assertNotIn(value, ba_vals)
            ba_vals.extend(value)
        log.debug("test_merge_overwrite(): Values for the 'time' variable: %s" % ba_vals)


        
        # Step 2: Now try ingesting some supplements which overlap
        #---------------------------------------------------------
        log.info('test_merge_overwrite(): Merging second supplement (3 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=send_as_chunks, supplement_number=2, supplement_overlap_count=3)
        
        log.info('test_merge_overwrite(): Merging third supplement (tail data -- to check for origin updates)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=send_as_chunks, supplement_number=3)
        
        log.info('test_merge_overwrite(): Merging fourth supplement (overwrite data in middle of dataset)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=send_as_chunks, supplement_number=1)
        
        
        
        # Test the data in the datastore
        dataset = yield self.ingest.rc.get_instance(dataset_id, excluded_types=[])
        root = dataset.root_group
        tvar = root.FindVariableByName('time')
        
        
        ba_vals  = []
        for ba in tvar.content.bounded_arrays:
            value = ba.ndarray.value
            self.assertNotIn(value, ba_vals)
            ba_vals.extend(value)
        ba_vals.sort()
        log.debug("test_merge_overwrite(): Sorted values for the 'time' variable: %s" % ba_vals)
        
        
        if log.getEffectiveLevel() <= logging.DEBUG:
            for i in range(len(tvar.content.bounded_arrays)):
                ba = tvar.content.bounded_arrays[i]
                log.debug('BA:%i  origin:%s  size:%s  values:%s' % (i, str([bound.origin for bound in ba.bounds]), str([bound.size for bound in ba.bounds]), str([(ba_vals.index(val), val) for val in ba.ndarray.value])))
        
        self.assertEqual(len(tvar.content.bounded_arrays), 5)
        log.debug('test_merge_overwrite(): Bounded array count: 4')
        
        
        # Verify the offsets for all the bounded arrays make sense
        self.assertEqual(len(tvar.content.bounded_arrays[0].bounds), 1)
        self.assertEqual(len(tvar.content.bounded_arrays[1].bounds), 1)
        self.assertEqual(len(tvar.content.bounded_arrays[2].bounds), 1)
        self.assertEqual(len(tvar.content.bounded_arrays[3].bounds), 1)
        self.assertEqual(len(tvar.content.bounded_arrays[4].bounds), 1)
        
        self.assertEqual(tvar.content.bounded_arrays[0].bounds[0].origin, 0)
        self.assertEqual(tvar.content.bounded_arrays[0].bounds[0].size, 1)

        self.assertEqual(tvar.content.bounded_arrays[1].bounds[0].origin, 6)
        self.assertEqual(tvar.content.bounded_arrays[1].bounds[0].size, 2)
        
        self.assertEqual(tvar.content.bounded_arrays[2].bounds[0].origin, 1)
        self.assertEqual(tvar.content.bounded_arrays[2].bounds[0].size, 1)
        
        self.assertEqual(tvar.content.bounded_arrays[3].bounds[0].origin, 4)   # bounded_arrays will have been switched during overwrite
        self.assertEqual(tvar.content.bounded_arrays[3].bounds[0].size, 2)     # -- this is expected
        
        self.assertEqual(tvar.content.bounded_arrays[4].bounds[0].origin, 2)
        self.assertEqual(tvar.content.bounded_arrays[4].bounds[0].size, 2)
        
        
        # Verify the number of reported values matches the number of actual values
        for ba in tvar.content.bounded_arrays:
            self.assertEqual(len(ba.ndarray.value), ba.bounds[0].size)
        
        
        # Verify that all values are monotonic and there are no duplicate indices (duplicates should have been replaced)
        value_list = []
        for i in range(len(tvar.content.bounded_arrays)):
            ba = tvar.content.bounded_arrays[i]
            for j in range(len(ba.ndarray.value)):
                val = ba.ndarray.value[j]
                key = ba.bounds[0].origin + j
                value_list.append((key, val))
        
        value_list.sort()
        last_key, last_val = value_list[0]
        for k,v in value_list[1:]:
            self.assertNotEqual(k, last_key, 'Indices should not match -- there should be no overlapping data')
            
            self.assertTrue(v > last_val, 'Fails if the data in the "time" variable is not monotonic after merge')
            last_key = k
            last_val = v
            
            
        #  Verify that the ion_time_coverage_start and ion_time_coverage_end match the min/max values of the bounded_arrays
        start_time_s = root.FindAttributeByName('ion_time_coverage_start')
        start_time   = calendar.timegm(time.strptime(start_time_s.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
        
        end_time_s = root.FindAttributeByName('ion_time_coverage_end')
        end_time   = calendar.timegm(time.strptime(end_time_s.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
        
        self.assertEqual(long(start_time), tvar.content.bounded_arrays[0].ndarray.value[0])
        self.assertEqual(long(end_time),   tvar.content.bounded_arrays[1].ndarray.value[-1])


        # @todo: Validate the arrangement of the salinity data mathematically?  (this should be possible because it is now created mathematically
    
    
    @defer.inlineCallbacks
    def test_merge_overwrite_check_fetch_intersects(self):
        """
        This is a test method for the merge dataset operation (via overwrite) of the ingestion service.
        This runs through an overwrite test where the supplement intersects the existing data.  This is
        done without first fetching the blobs for the dataset to ensure that the merge methods will
        fetch blobs when necessary.
        
        In this test the dataset is sent in totality as opposed to in chunks to test merging when the supplement
        is introduced by differing code-paths
        """
        log.info('test_merge_overwrite_check_fetch(): Merging first supplement (1 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=False, supplement_number=1, supplement_overlap_count=1)
    
    
    @defer.inlineCallbacks
    def test_merge_overwrite_check_fetch_intersects_chunks(self):
        """
        This is a test method for the merge dataset operation (via overwrite) of the ingestion service.
        This runs through an overwrite test where the supplement intersects the existing data.  This is
        done without first fetching the blobs for the dataset to ensure that the merge methods will
        fetch blobs when necessary.
        
        In this test the dataset is sent in chunks as opposed to in totality to test merging when the supplement
        is introduced by differing code-paths
        """
        log.info('test_merge_overwrite_check_fetch(): Merging first supplement (1 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=True, supplement_number=1, supplement_overlap_count=1)
    
    
    @defer.inlineCallbacks
    def test_merge_overwrite_check_fetch_contains(self):
        """
        This is a test method for the merge dataset operation (via overwrite) of the ingestion service.
        This runs through an overwrite test where the supplement fully contains the existing data.  This is
        done without first fetching the blobs for the dataset to ensure that the merge methods will
        fetch blobs when necessary
        
        In this test the dataset is sent in totality as opposed to in chunks to test merging when the supplement
        is introduced by differing code-paths
        """

        log.info('test_merge_overwrite_check_fetch(): Merging supplement (4 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=False, supplement_number=2, supplement_overlap_count=4)
    
    
    @defer.inlineCallbacks
    def test_merge_overwrite_check_fetch_contains_chunks(self):
        """
        This is a test method for the merge dataset operation (via overwrite) of the ingestion service.
        This runs through an overwrite test where the supplement fully contains the existing data.  This is
        done without first fetching the blobs for the dataset to ensure that the merge methods will
        fetch blobs when necessary
        
        In this test the dataset is sent in chunks as opposed to in totality to test merging when the supplement
        is introduced by differing code-paths
        """
        log.info('test_merge_overwrite_check_fetch(): Merging supplement (4 timestep overlap)')
        dataset_id = yield self._perform_test_ingest(aggregation_rule=AggregationRule.OVERWRITE, send_as_chunks=True, supplement_number=2, supplement_overlap_count=4)


    @defer.inlineCallbacks
    def _perform_test_ingest(self, aggregation_rule=None, send_as_chunks=False, *args, **kwargs):
        """
        @param kwargs: Key-worded arguments to be sent to bootstrap_profile_dataset() when building the sample dataset for ingestion
        """
        # Receive a dataset to get setup...
        content               = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id    = SAMPLE_PROFILE_DATASET_ID
        content.datasource_id = SAMPLE_PROFILE_DATA_SOURCE_ID
        yield self.ingest._prepare_ingest(content)
        
        self.ingest.timeoutcb = create_delayed_call()


        # Set the aggregation_rule if present
        if aggregation_rule is AggregationRule.OVERLAP:
            self.ingest.data_source.aggregation_rule = self.ingest.data_source.AggregationRule.OVERLAP
        if aggregation_rule is AggregationRule.OVERWRITE:
            self.ingest.data_source.aggregation_rule = self.ingest.data_source.AggregationRule.OVERWRITE
        if aggregation_rule is AggregationRule.FMRC:
            self.ingest.data_source.aggregation_rule = self.ingest.data_source.AggregationRule.FMRC
        yield self.ingest.rc.put_instance(self.ingest.data_source)
        
        
        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        bootstrap_profile_dataset(cdm_dset_msg, *args, **kwargs)
        
        # @todo: If specified, pull the cdm_dset_msg apart and send it as a shell and
        # subsequently send the data as chunks..
        if send_as_chunks:
            
            # Remove all bounded arrays from the dataset message to create an empty "shell"
            var_dict = {}
            for var in cdm_dset_msg.root_group.variables:

                var_name = var.name
                ba_list = []
                for i in range(len(var.content.bounded_arrays) - 1, -1, -1):  # Iterate over the list in reverse so we can remove values
                    ba = var.content.bounded_arrays[i]
                    del  var.content.bounded_arrays[i]
                    ba_list.insert(0, ba)
                    
                    
                var_dict[var_name] = ba_list
            
            for k, v in var_dict.iteritems():
                log.debug('%s%s%s' % (k, ':', v))
                
            # Send the cdm dataset message as a dataset "shell"
            yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())
            
            
            # Send each bounded array as a separate data "chunk"
            for k, v in var_dict.iteritems():
                for ba in v:
                    cdm_supl_msg = yield self.ingest.mc.create_instance(SUPPLEMENT_MSG_TYPE)
                    cdm_supl_msg.variable_name = k
                    cdm_supl_msg.bounded_array = ba
                    cdm_supl_msg.Repository.commit('Storing bounded array chunk for var "%s" before passing to _ingest_op_recv_chunk' % k)
                    yield self.ingest._ingest_op_recv_chunk(cdm_supl_msg, '', self.fake_msg())
                
        else:

            # Send the dataset "chunk" message -- Call the op of the ingest process directly
            yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())


        # Send the dataset "done" message
        complete_msg = yield self.ingest.mc.create_instance(DAQ_COMPLETE_MSG_TYPE)
        complete_msg.status = complete_msg.StatusCode.OK

        yield self.ingest._ingest_op_recv_done(complete_msg, '', self.fake_msg())


        #####  This cleanup code from op_ingest must be manually invoked because since we
        #####  are running ops directly, normal process flow does not occur.  This causes
        #####  adverse things to happen during testing, such as, the dataset not being
        #####  fully pushed to the datastore and the ingestion defered not being reset...
        ############################################# 
        self.ingest._defer_ingest = defer.Deferred()
        
        if self.ingest.dataset.ResourceLifeCycleState != self.ingest.dataset.ACTIVE:
            self.ingest.dataset.ResourceLifeCycleState = self.ingest.dataset.ACTIVE
            
        yield self.ingest.rc.put_instance(self.ingest.dataset)
        
        self.ingest.dataset = None
        self.ingest.data_source = None
        #############################################
        
        
        defer.returnValue(content.dataset_id)


    @defer.inlineCallbacks
    def _create_datasource_and_set(self, new_dataset_id, new_datasource_id):

        def create_dataset(dataset, *args, **kwargs):
            """
            Create an empty dataset
            """
            group = dataset.CreateObject(GROUP_TYPE)
            dataset.root_group = group
            return True

        data_set_description = {ID_CFG:new_dataset_id,
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:'Blank dataset for testing ingestion',
                      DESCRIPTION_CFG:'An example of a station dataset',
                      CONTENT_CFG:create_dataset,
                      }

        self.datastore._create_resource(data_set_description)

        dset_res = self.datastore.workbench.get_repository(new_dataset_id)

        log.info('Created Dataset Resource for test.')

        def create_datasource(datasource, *args, **kwargs):
            """
            Create an empty dataset
            """
            datasource.source_type = datasource.SourceType.NETCDF_S
            datasource.request_type = datasource.RequestType.DAP

            datasource.base_url = "http://not_a_real_url.edu"

            datasource.max_ingest_millis = 6000

            datasource.registration_datetime_millis = IonTime().time_ms

            datasource.ion_title = "NTAS1 Data Source"
            datasource.ion_description = "Data NTAS1"

            datasource.aggregation_rule = datasource.AggregationRule.OVERLAP

            return True


        data_source_description = {ID_CFG:new_datasource_id,
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:'datasource for testing ingestion',
                      DESCRIPTION_CFG:'An example of a station datasource',
                      CONTENT_CFG:create_datasource,
                      }

        self.datastore._create_resource(data_source_description)

        dsource_res = self.datastore.workbench.get_repository(new_datasource_id)

        log.info('Created Datasource Resource for test.')

        yield self.datastore.workbench.flush_repo_to_backend(dset_res)
        yield self.datastore.workbench.flush_repo_to_backend(dsource_res)

        log.info('Data resources flushed to backend')
        defer.returnValue((dset_res, dsource_res))


    @defer.inlineCallbacks
    def test_ingest_on_new_dataset(self):
        """
        This is a test method for the recv dataset operation of the ingestion service
        """

        new_dataset_id = 'C37A2796-E44C-47BF-BBFB-637339CE81D0'
        new_datasource_id = '0B1B4D49-6C64-452F-989A-2CDB02561BBE'
        yield self._create_datasource_and_set(new_dataset_id, new_datasource_id)

        # Receive a dataset to get setup...
        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = new_dataset_id
        content.datasource_id = new_datasource_id

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = create_delayed_call()

        # Now fake the receipt of the dataset message
        cdm_dset_msg = yield self.ingest.mc.create_instance(CDM_DATASET_TYPE)
        bootstrap_profile_dataset(cdm_dset_msg, supplement_number=1, random_initialization=True)

        log.info('Calling Receive Dataset')

        # Call the op of the ingest process directly
        yield self.ingest._ingest_op_recv_dataset(cdm_dset_msg, '', self.fake_msg())

        log.info('Calling Receive Dataset: Complete')

        complete_msg = yield self.ingest.mc.create_instance(DAQ_COMPLETE_MSG_TYPE)

        log.info('Calling Receive Done')

        complete_msg.status = complete_msg.StatusCode.OK
        yield self.ingest._ingest_op_recv_done(complete_msg, '', self.fake_msg())

        log.info('Calling Receive Done: Complete!')





    @defer.inlineCallbacks
    def test_notify(self):

        ### Test the unavailable notification
        sub_unavailable = DatasourceUnavailableEventSubscriber(process=self.proc, origin=SAMPLE_PROFILE_DATA_SOURCE_ID)
        yield sub_unavailable.initialize()
        yield sub_unavailable.activate()

        test_deferred = defer.Deferred()

        sub_unavailable.ondata = lambda msg: test_deferred.callback( msg['content'].additional_data.error_explanation)

        data_details = {EM_TITLE:'title',
                       EM_URL:'references',
                       EM_DATA_SOURCE:SAMPLE_PROFILE_DATA_SOURCE_ID,
                       EM_DATASET:SAMPLE_PROFILE_DATASET_ID,
                       EM_ERROR:'ERROR # 1',
                       }
        yield self.ingest._notify_ingest(data_details)
        errors_received = yield test_deferred

        self.assertEqual(errors_received, 'ERROR # 1')


        test_deferred = defer.Deferred()

        data_details = {EM_TITLE:'title',
                       EM_URL:'references',
                       EM_DATA_SOURCE:SAMPLE_PROFILE_DATA_SOURCE_ID,
                       EM_DATASET:SAMPLE_PROFILE_DATASET_ID,
                       EM_ERROR:'ERROR # 2',
                       }
        yield self.ingest._notify_ingest(data_details)
        errors_received = yield test_deferred

        self.assertEqual(errors_received, 'ERROR # 2')


        ### Test the Data Supplement notification
        sub_added = DatasetSupplementAddedEventSubscriber(process=self.proc, origin=SAMPLE_PROFILE_DATASET_ID)
        yield sub_added.initialize()
        yield sub_added.activate()

        sub_added.ondata = lambda msg: test_deferred.callback( msg['content'].additional_data.number_of_timesteps)

        test_deferred = defer.Deferred()

        data_details = {EM_TITLE:'title',
                        EM_URL:'references',
                        EM_DATA_SOURCE:SAMPLE_PROFILE_DATA_SOURCE_ID,
                        EM_DATASET:SAMPLE_PROFILE_DATASET_ID,
                        EM_START_DATE:59,
                        EM_END_DATE:69,
                        EM_TIMESTEPS:7
                        }
        yield self.ingest._notify_ingest(data_details)
        nsteps = yield test_deferred

        self.assertEqual(nsteps, 7)

    @defer.inlineCallbacks
    def test_error_in_ingest(self):
        """
        Attempts to raise an error during the ingestion process to ensure they are trapped and
        reported properly.  We are simulating JAW/DatasetAgent interaction and do a simple "incorrect message type"
        to the first sub-ingestion method.
        """

        new_dataset_id = 'C37A2796-E44C-47BF-BBFB-637339CE81D0'
        new_datasource_id = '0B1B4D49-6C64-452F-989A-2CDB02561BBE'
        yield self._create_datasource_and_set(new_dataset_id, new_datasource_id)

        # now, start ingestion on this fake dataset
        msg = yield self.proc.message_client.create_instance(PERFORM_INGEST_MSG_TYPE)
        msg.dataset_id = new_dataset_id
        msg.reply_to = "fake.respond"
        msg.ingest_service_timeout = 45
        msg.datasource_id = new_datasource_id

        # get a subscriber going for the ingestion ready message
        def_ready = defer.Deferred()
        def readyrecv(data):
            def_ready.callback(True)

        readysub = Subscriber(xp_name="magnet.topic",
                              binding_key="fake.respond",
                              process=self.proc)
        readysub.ondata = readyrecv
        yield readysub.initialize()
        yield readysub.activate()

        # start ingestion, hold its deferred as we need to do something with it in a bit
        ingestdef = self._ic.ingest(msg)

        # wait for ready response from ingestion
        yield def_ready

        log.info("Ready response from ingestion, proceeding to give it an incorrect message type to recv_chunk")

        # now send it an incorrect message, make sure we get an error back
        badmsg = yield self.proc.message_client.create_instance(SUPPLEMENT_MSG_TYPE)

        pub = Publisher(process=self.proc,
                        xp_name=get_events_exchange_point(),
                        routing_key="%s.%s" % (str(DATASET_STREAMING_EVENT_ID), new_dataset_id))

        yield pub.initialize()
        yield pub.activate()

        # yuck, can't use pub.publish, it won't let us set an op
        kwargs = { 'recipient' : pub._routing_key,
                   'content'   : badmsg,
                   'headers'   : {'sender-name' : self.proc.proc_name },
                   'operation' : 'recv_dataset',
                   'sender'    : self.proc.id.full }

        yield pub._recv.send(**kwargs)

        yield self.failUnlessFailure(ingestdef, ReceivedApplicationError)

        # check called back thing
        self.failUnless("Expected message type" in ingestdef.result.msg_content.MessageResponseBody)
        self.failUnless(ingestdef.result.msg_content.MessageResponseCode, msg.ResponseCodes.BAD_REQUEST)

    @defer.inlineCallbacks
    def test_recv_random_order(self):
        """
        Tests a variable in a dataset can be sent in many chunks in any order and it assembles properly.
        """
        new_dataset_id = 'C37A2796-E44C-47BF-BBFB-637339CE81D0'
        new_datasource_id = '0B1B4D49-6C64-452F-989A-2CDB02561BBE'
        yield self._create_datasource_and_set(new_dataset_id, new_datasource_id)

        dataset = yield self.rc.get_instance(new_dataset_id)
        #datasource = yield self.rc.get_instance(content.datasource_id)

        # add our variable
        var_name = "depth"
        float_type = dataset.root_group.DataType.FLOAT
        ddim = dataset.root_group.AddDimension(var_name, 100, False)
        var = dataset.root_group.AddVariable(var_name, float_type, [ddim])
        var.content = dataset.CreateObject(ARRAY_STRUCTURE_TYPE)

        # push this back
        yield self.rc.put_instance(dataset, "Added depth variable")

        # boilerplate for starting ingestion
        content = yield self.ingest.mc.create_instance(PERFORM_INGEST_MSG_TYPE)
        content.dataset_id = new_dataset_id
        content.datasource_id = new_datasource_id

        yield self.ingest._prepare_ingest(content)

        self.ingest.timeoutcb = create_delayed_call()

        self.ingest.dataset.CreateUpdateBranch()

        # swap out our reference to var to be the one ingest is working with (so we don't have to pull again)
        group = self.ingest.dataset.root_group
        var = group.FindVariableByName(var_name)

        # start creating chunks of the same variable!
        starting_bounded_arrays  = var.content.bounded_arrays[:]

        log.debug("num bas %d" % len(starting_bounded_arrays))

        shuflist = [x for x in xrange(10)]
        random.shuffle(shuflist)
        for x in shuflist:
            log.debug("Creating depth chunk %d" % x)
            supplement_msg = yield self.ingest.mc.create_instance(SUPPLEMENT_MSG_TYPE)
            supplement_msg.dataset_id = SAMPLE_PROFILE_DATASET_ID
            supplement_msg.variable_name = var_name

            supplement_msg.bounded_array = supplement_msg.CreateObject(BOUNDED_ARRAY_TYPE)
            supplement_msg.bounded_array.ndarray = supplement_msg.CreateObject(FLOAT32ARRAY_TYPE)

            # set bounds
            supplement_msg.bounded_array.bounds.add()
            supplement_msg.bounded_array.bounds[0].origin = x * 10
            supplement_msg.bounded_array.bounds[0].size = 10

            # set data
            supplement_msg.bounded_array.ndarray.value.extend([y/10.0 for y in range(x*10, x*10+10)])

            # commit!
            supplement_msg.Repository.commit("committing round %d" % x)

            # Call the op of the ingest process directly
            yield self.ingest._ingest_op_recv_chunk(supplement_msg, '', self.fake_msg())

        updated_bounded_arrays = var.content.bounded_arrays[:]

        # should add 10 bounded arrays
        self.failUnlessEqual(len(updated_bounded_arrays), len(starting_bounded_arrays)+10)

        # should be able to iterate linearly using GetValue
        for x in xrange(100):
            self.failUnlessApproximates(x/10.0, var.GetValue(x), 0.01)  # precision may be an issue here?
            log.debug("Value %d: %f" % (x, var.GetValue(x)))
