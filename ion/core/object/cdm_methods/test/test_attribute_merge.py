#!/usr/bin/env python

"""
@file ion/services/dm
@author Tim LaRocque
@brief test for eoi ingestion demo
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.test.iontest import IonTestCase
from ion.core.object.object_utils import create_type_identifier
from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceInstance
from ion.services.coi.datastore_bootstrap.ion_preload_config import PRELOAD_CFG, ION_DATASETS_CFG

CDM_DATASET_TYPE = create_type_identifier(object_id=10001, version=1)

from ion.core.object.cdm_methods.attribute_merge import MergeAttSrc, MergeAttDst, MergeAttGreater, MergeAttLesser

class CdmAttributeTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        #================================
        # Start required services:
        #================================
        ds_spawn_args = {PRELOAD_CFG:{ION_DATASETS_CFG:False}}
        
        services = [
            {
             'name':'datastore_service',
             'module':'ion.services.coi.datastore',
             'class':'DataStoreService',
             'spawnargs':ds_spawn_args
            },
            {
             'name':'resource_registry_service',
             'module':'ion.services.coi.resource_registry.resource_registry',
             'class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {
             'name':'association_service',
             'module':'ion.services.dm.inventory.association_service',
             'class':'AssociationService'
            }
        ]
        
        sup = yield self._spawn_processes(services)
        self.sup = sup
        
        
        #================================
        # Create the demo dataset(s)
        #================================
        rc = ResourceClient(proc=sup)
        ds = yield rc.create_instance(CDM_DATASET_TYPE, ResourceName='Test CDM dataset resource')
        
        self.ds = ds.ResourceObject
        self.ds.MakeRootGroup()
        
        self.group1 = self.ds.root_group
        self.group2 = self.group1.AddGroup('new_group')
                
        
    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()

# -------------------------------------------------- #
# --------------- MergeAttSrc ---------------------- #
# -------------------------------------------------- #
    def test_MergeAttSrc_no_src(self):
        self.group1.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 'This is a test')
        self.assertTrue(self.group2.HasAttribute('test') == False)
        
        MergeAttSrc(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 'This is a test')
        self.assertTrue(self.group2.HasAttribute('test') == False)

    def test_MergeAttSrc_no_dst(self):
        self.group2.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.assertTrue(self.group1.HasAttribute('test') == False)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 'This is a test')
        
        MergeAttSrc(self.group1, 'test', self.group2)
        
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 'This is a test')
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 'This is a test')

    def test_MergeAttSrc_same_type(self):
        self.group1.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.group2.AddAttribute('test', self.group1.DataType.STRING, 'NEW VALUE')
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 'This is a test')
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 'NEW VALUE')
        
        MergeAttSrc(self.group1, 'test', self.group2)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 'NEW VALUE')
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 'NEW VALUE')

    def test_MergeAttSrc_dif_type(self):
        self.group1.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.group2.AddAttribute('test', self.group1.DataType.FLOAT, 123.456)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 'This is a test')
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 123.456)
        
        MergeAttSrc(self.group1, 'test', self.group2)
        
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)

    def test_MergeAttSrc_same_val(self):
        self.group1.AddAttribute('test', self.group1.DataType.BYTE, 238)
        self.group2.AddAttribute('test', self.group1.DataType.BYTE, 238)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 238)
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 238)
        
        # Ensure there are no unexpected changes to the attribute values
        MergeAttSrc(self.group1, 'test', self.group2)
        
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 238)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 238)

# -------------------------------------------------- #
# --------------- MergeAttDst ---------------------- #
# -------------------------------------------------- #

    def test_MergeAttDst_no_src(self):
        self.group1.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 'This is a test')
        self.assertTrue(self.group2.HasAttribute('test') == False)
        
        MergeAttDst(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 'This is a test')
        self.assertTrue(self.group2.HasAttribute('test') == False)

    def test_MergeAttDst_no_dst(self):
        self.group2.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.assertTrue(self.group1.HasAttribute('test') == False)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 'This is a test')
        
        MergeAttDst(self.group1, 'test', self.group2)
        
        self.assertTrue(self.group1.HasAttribute('test') == False)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 'This is a test')

    def test_MergeAttDst_same_type(self):
        self.group1.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.group2.AddAttribute('test', self.group1.DataType.STRING, 'NEW VALUE')
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 'This is a test')
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 'NEW VALUE')
        
        MergeAttDst(self.group1, 'test', self.group2)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 'This is a test')
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 'NEW VALUE')

    def test_MergeAttDst_dif_type(self):
        self.group1.AddAttribute('test', self.group1.DataType.STRING, 'This is a test')
        self.group2.AddAttribute('test', self.group1.DataType.FLOAT, 123.456)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 'This is a test')
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 123.456)
        
        MergeAttDst(self.group1, 'test', self.group2)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 'This is a test')
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 123.456)
        
    def test_MergeAttDst_same_val(self):
        self.group1.AddAttribute('test', self.group1.DataType.BYTE, 238)
        self.group2.AddAttribute('test', self.group1.DataType.BYTE, 238)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 238)
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 238)
        
        # Ensure there are no unexpected changes to the attribute values
        MergeAttDst(self.group1, 'test', self.group2)
        
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 238)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 238)

# ------------------------------------------------------ #
# --------------- MergeAttGreater ---------------------- #
# ------------------------------------------------------ #

    def test_MergeAttGreater_no_src(self):
        self.group1.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.HasAttribute('test') == False)
        
        MergeAttGreater(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.HasAttribute('test') == False)

    def test_MergeAttGreater_no_dst(self):
        self.group2.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.assertTrue(self.group1.HasAttribute('test') == False)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)
        
        MergeAttGreater(self.group2, 'test', self.group1)
        self.assertTrue(self.group1.HasAttribute('test') == False)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)

        MergeAttGreater(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)

    def test_MergeAttGreater_src_greater(self):
        self.group1.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.group2.AddAttribute('test', self.group1.DataType.DOUBLE, 200.12345)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 200.12345)
        
        MergeAttGreater(self.group2, 'test', self.group1)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 200.12345)

        MergeAttGreater(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 200.12345)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 200.12345)

    def test_MergeAttGreater_dst_greater(self):
        self.group1.AddAttribute('test', self.group1.DataType.DOUBLE, 200.12345)
        self.group2.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 200.12345)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)
        
        MergeAttGreater(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 200.12345)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)
    
    def test_MergeAttGreater_same_val(self):
        self.group1.AddAttribute('test', self.group1.DataType.BYTE, 238)
        self.group2.AddAttribute('test', self.group1.DataType.BYTE, 238)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 238)
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 238)
        
        # Ensure there are no unexpected changes to the attribute values
        MergeAttGreater(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 238)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 238)
        
    def _do_test_MergeAttGreater_double_against_numeric(self, lesser, greater, lesser_type, greater_type):
        
        # Step 1: Make sure lesser and greater values are not the same
        self.assertNotEqual(lesser, greater)
        
        # Step 2: Store the values in two of the same attributes in different groups (ensure they get set)
        self.group1.AddAttribute('test', lesser_type, lesser)
        self.group2.AddAttribute('test', greater_type, greater)
        self.assertEqual(self.group1.FindAttributeByName('test').GetValue(), lesser)
        self.assertEqual(self.group2.FindAttributeByName('test').GetValue(), greater)
        
        # Step 3: Use Merge Greater into DST when DST is already greater (ensure NO-OP)
        MergeAttGreater(self.group2, 'test', self.group1)
        self.assertEqual(self.group1.FindAttributeByName('test').GetValue(), lesser)
        self.assertEqual(self.group2.FindAttributeByName('test').GetValue(), greater)

        # Step 4: Use Merge Greater into DST when SRC is greater (ensure overwrite)
        MergeAttGreater(self.group1, 'test', self.group2)
        self.assertEqual(self.group1.FindAttributeByName('test').GetValue(), greater)
        self.assertEqual(self.group2.FindAttributeByName('test').GetValue(), greater)
        self.assertEqual(self.group1.FindAttributeByName('test').data_type, greater_type)
        
        # Step 5: Cleanup
        self.group1.RemoveAttribute('test')
        self.group2.RemoveAttribute('test')

    def test_MergeAttGreater_double_and_byte(self):
        lesser = (2**8) - 2 + 0.54321
        greater = (2**8) - 1
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.BYTE)

    def test_MergeAttGreater_double_and_short(self):
        lesser = (2**15) - 2 + 0.54321
        greater = (2**15) - 1
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.SHORT)

    def test_MergeAttGreater_double_and_int(self):
        lesser = (2**31) - 2 + 0.54321
        greater = (2**31) - 1
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.INT)

    def test_MergeAttGreater_double_and_long(self):
        # @attention: won't support 2**63 for some reason
        lesser = (2**54) - 2 + 0.54321
        greater = (2**54) - 1
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.LONG)

    def test_MergeAttGreater_double_and_float(self):
        lesser = (2**31) - 2 + 0.1234
        greater = (2**31) - 2 + 0.9876
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.FLOAT)

    def test_MergeAttGreater_double_and_double(self):
        lesser = (2**31) - 2 + 0.123456789
        greater = (2**31) - 2 + 0.987654321
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.DOUBLE)
    
    def test_MergeAttGreater_double_and_char(self):
        lesser = ord('a')
        greater = 'b'
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.CHAR)
    
    def test_MergeAttGreater_double_and_string_int(self):
        lesser = 123.456
        greater = '234'
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.STRING)
        
    def test_MergeAttGreater_long_and_string_long(self):
        lesser = -4294967296
        greater = '-4294967295'
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.LONG, self.group1.DataType.STRING)

    def test_MergeAttGreater_float_and_string_float(self):
        lesser = 123.456
        greater = '123.567'
        
        self._do_test_MergeAttGreater_double_against_numeric(lesser, greater, self.group1.DataType.FLOAT, self.group1.DataType.STRING)

    def test_MergeAttGreater_double_and_string_time(self):
        time_epoch = 0.0
        time_day_after_epoch = '1970-01-02T00:00:00Z'
        
        self._do_test_MergeAttGreater_double_against_numeric(time_epoch, time_day_after_epoch, self.group1.DataType.DOUBLE, self.group1.DataType.STRING)

    def test_MergeAttGreater_string_time_and_string_time(self):
        # Differ times by year
        time_lesser = '2010-04-12T00:00:00Z'
        time_greater = '2011-04-12T00:00:00Z'
        
        self._do_test_MergeAttGreater_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by month
        time_lesser = '2011-03-12T00:00:00Z'
        time_greater = '2011-04-12T00:00:00Z'
        
        self._do_test_MergeAttGreater_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by day
        time_lesser = '2011-04-11T00:00:00Z'
        time_greater = '2011-04-12T00:00:00Z'
        
        self._do_test_MergeAttGreater_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by hour
        time_lesser = '2011-04-12T00:00:00Z'
        time_greater = '2011-04-12T01:00:00Z'
        
        self._do_test_MergeAttGreater_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by minute
        time_lesser = '2011-04-12T00:00:00Z'
        time_greater = '2011-04-12T00:01:00Z'
        
        self._do_test_MergeAttGreater_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by second
        time_lesser = '2011-04-12T00:00:00Z'
        time_greater = '2011-04-12T00:00:01Z'
        
        self._do_test_MergeAttGreater_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

# ------------------------------------------------------ #
# --------------- MergeAttGreater ---------------------- #
# ------------------------------------------------------ #

    def test_MergeAttLesser_no_src(self):
        self.group1.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.HasAttribute('test') == False)
        
        MergeAttLesser(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.HasAttribute('test') == False)

    def test_MergeAttLesser_no_dst(self):
        self.group2.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.assertTrue(self.group1.HasAttribute('test') == False)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)
        
        MergeAttLesser(self.group2, 'test', self.group1)
        self.assertTrue(self.group1.HasAttribute('test') == False)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)

        MergeAttLesser(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)

    def test_MergeAttLesser_src_lesser(self):
        self.group1.AddAttribute('test', self.group1.DataType.DOUBLE, 200.12345)
        self.group2.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 200.12345)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)
        
        MergeAttLesser(self.group2, 'test', self.group1)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 200.12345)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)

        MergeAttLesser(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 123.456)

    def test_MergeAttLesser_dst_lesser(self):
        self.group1.AddAttribute('test', self.group1.DataType.DOUBLE, 123.456)
        self.group2.AddAttribute('test', self.group1.DataType.DOUBLE, 200.12345)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 200.12345)
        
        MergeAttLesser(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 123.456)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 200.12345)

    def test_MergeAttLesser_same_val(self):
        self.group1.AddAttribute('test', self.group1.DataType.BYTE, 238)
        self.group2.AddAttribute('test', self.group1.DataType.BYTE, 238)
        
        self.assertEquals(self.group1.FindAttributeByName('test').GetValue(), 238)
        self.assertEquals(self.group2.FindAttributeByName('test').GetValue(), 238)
        
        # Ensure there are no unexpected changes to the attribute values
        MergeAttLesser(self.group1, 'test', self.group2)
        self.assertTrue(self.group1.FindAttributeByName('test').GetValue() == 238)
        self.assertTrue(self.group2.FindAttributeByName('test').GetValue() == 238)
        
    def _do_test_MergeAttLesser_double_against_numeric(self, lesser, greater, lesser_type, greater_type):
        
        # Step 1: Make sure lesser and greater values are not the same
        self.assertNotEqual(lesser, greater)
        
        # Step 2: Store the values in two of the same attributes in different groups (ensure they get set)
        self.group1.AddAttribute('test', lesser_type, lesser)
        self.group2.AddAttribute('test', greater_type, greater)
        self.assertEqual(self.group1.FindAttributeByName('test').GetValue(), lesser)
        self.assertEqual(self.group2.FindAttributeByName('test').GetValue(), greater)
        
        # Step 3: Use Merge Lesser into DST when DST is already lesser (ensure NO-OP)
        MergeAttLesser(self.group1, 'test', self.group2)
        self.assertEqual(self.group1.FindAttributeByName('test').GetValue(), lesser)
        self.assertEqual(self.group2.FindAttributeByName('test').GetValue(), greater)

        # Step 4: Use Merge Lesser into DST when SRC is lesser (ensure overwrite)
        MergeAttLesser(self.group2, 'test', self.group1)
        self.assertEqual(self.group1.FindAttributeByName('test').GetValue(), lesser)
        self.assertEqual(self.group2.FindAttributeByName('test').GetValue(), lesser)
        self.assertEqual(self.group1.FindAttributeByName('test').data_type, lesser_type)
        
        # Step 5: Cleanup
        self.group1.RemoveAttribute('test')
        self.group2.RemoveAttribute('test')

    def test_MergeAttLesser_double_and_byte(self):
        lesser = (2**8) - 2
        greater = (2**8) - 2 + 0.54321
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.BYTE, self.group1.DataType.DOUBLE)

    def test_MergeAttLesser_double_and_short(self):
        lesser = (2**15) - 2
        greater = (2**15) - 2 + 0.54321
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.SHORT, self.group1.DataType.DOUBLE)

    def test_MergeAttLesser_double_and_int(self):
        lesser = (2**31) - 2
        greater = (2**31) - 2 + 0.54321
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.INT, self.group1.DataType.DOUBLE)

    def test_MergeAttLesser_double_and_long(self):
        # @attention: Cannot test a double value around 2**63 - 1 against a long value around 2**63 - 1
        #             because the double will loose precision
        # Example:
        #        (2**2 + 0.54321) == 2**2        FALSE
        #        (2**63 + 0.54321) == 2**63      TRUE  (because precision is lost)
        #
        lesser = (2**31) - 2
        greater = (2**31) - 2 + 0.54321
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.LONG, self.group1.DataType.DOUBLE)

    def test_MergeAttLesser_double_and_float(self):
        lesser = (2**31) - 2 + 0.1234
        greater = (2**31) - 2 + 0.9876
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.FLOAT, self.group1.DataType.DOUBLE)

    def test_MergeAttLesser_double_and_double(self):
        lesser = (2**31) - 2 + 0.123456789
        greater = (2**31) - 2 + 0.987654321
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.DOUBLE, self.group1.DataType.DOUBLE)
    
    def test_MergeAttLesser_double_and_char(self):
        lesser = 'a'
        greater = ord('b')
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.CHAR, self.group1.DataType.DOUBLE)
    
    def test_MergeAttLesser_double_and_string_int(self):
        lesser = '123'
        greater = 123.456
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.STRING, self.group1.DataType.DOUBLE)

    def test_MergeAttLesser_long_and_string_long(self):
        lesser = '-4294967296'
        greater = -4294967295
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.STRING, self.group1.DataType.LONG)

    def test_MergeAttLesser_float_and_string_float(self):
        lesser = '123.456'
        greater = 123.567
        
        self._do_test_MergeAttLesser_double_against_numeric(lesser, greater, self.group1.DataType.STRING, self.group1.DataType.FLOAT)

    def test_MergeAttLesser_double_and_string_time(self):
        time_day_before_epoch = '1969-12-31T00:00:00Z'
        time_epoch = 0.0
        
        self._do_test_MergeAttLesser_double_against_numeric(time_day_before_epoch, time_epoch, self.group1.DataType.STRING, self.group1.DataType.DOUBLE)

    def test_MergeAttLesser_string_time_and_string_time(self):
        # Differ times by year
        time_lesser = '2010-04-12T00:00:00Z'
        time_greater = '2011-04-12T00:00:00Z'
        
        self._do_test_MergeAttLesser_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by month
        time_lesser = '2011-03-12T00:00:00Z'
        time_greater = '2011-04-12T00:00:00Z'
        
        self._do_test_MergeAttLesser_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by day
        time_lesser = '2011-04-11T00:00:00Z'
        time_greater = '2011-04-12T00:00:00Z'
        
        self._do_test_MergeAttLesser_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by hour
        time_lesser = '2011-04-12T00:00:00Z'
        time_greater = '2011-04-12T01:00:00Z'
        
        self._do_test_MergeAttLesser_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by minute
        time_lesser = '2011-04-12T00:00:00Z'
        time_greater = '2011-04-12T00:01:00Z'
        
        self._do_test_MergeAttLesser_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)

        # Differ times by second
        time_lesser = '2011-04-12T00:00:00Z'
        time_greater = '2011-04-12T00:00:01Z'
        
        self._do_test_MergeAttLesser_double_against_numeric(time_lesser, time_greater, self.group1.DataType.STRING, self.group1.DataType.STRING)




