#!/usr/bin/env python

"""
@file ion/services/dm/ingestion
@author Tim LaRocque
@brief test for eoi ingestion demo
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.test.iontest import IonTestCase
from ion.core.object.object_utils import create_type_identifier
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import PRELOAD_CFG, ION_DATASETS_CFG

CDM_DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
CDM_ARRAY_STRUC_TYPE = create_type_identifier(object_id=10025, version=1)
CDM_BOUNDED_ARRAY_TYPE = create_type_identifier(object_id=10021, version=1)
CDM_F64_ARRAY_TYPE = create_type_identifier(object_id=10014, version=1)

from ion.core.object.cdm_methods.variables import _flatten_index

class CdmVariableTest(IonTestCase):
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
        self.rc = ResourceClient(proc=sup)
        ds = yield self.rc.create_instance(CDM_DATASET_TYPE, ResourceName='Test CDM dataset resource')
        
        self.ds = ds.ResourceObject
        self.ds.MakeRootGroup()
        
        self.root = self.ds.root_group
                
        
    @defer.inlineCallbacks
    def tearDown(self):
        # You must explicitly clear the registry in case cassandra is used as a back end!
        yield self._stop_container()
    
    @defer.inlineCallbacks
    def setup_1D_multiple_BA(self):
        dim1 = self.root.AddDimension('dim1', 90)
        var1 = self.root.AddVariable('var1', self.root.DataType.DOUBLE, [dim1])
        
        # Create the array structure
        content = yield var1.Repository.create_object(CDM_ARRAY_STRUC_TYPE)
        ba1 = yield content.Repository.create_object(CDM_BOUNDED_ARRAY_TYPE)
        ba2 = yield content.Repository.create_object(CDM_BOUNDED_ARRAY_TYPE)
        ba3 = yield content.Repository.create_object(CDM_BOUNDED_ARRAY_TYPE)
        arr1 = yield ba1.Repository.create_object(CDM_F64_ARRAY_TYPE)
        arr2 = yield ba2.Repository.create_object(CDM_F64_ARRAY_TYPE)
        arr3 = yield ba3.Repository.create_object(CDM_F64_ARRAY_TYPE)
        
        # Configure the array's bounds
        ba1.bounds.add()
        ba1.bounds[0].origin = 0
        ba1.bounds[0].size = 30
        ba2.bounds.add()
        ba2.bounds[0].origin = 30
        ba2.bounds[0].size = 30
        ba3.bounds.add()
        ba3.bounds[0].origin = 60
        ba3.bounds[0].size = 30
        
        # Insert values into the arrays
        arr1.value.extend([float(val) for val in range( 0,30)])
        arr2.value.extend([float(val) for val in range(30,60)])
        arr3.value.extend([float(val) for val in range(60,90)])
        
        # Put the pieces together
        ba1.ndarray = arr1
        ba2.ndarray = arr2
        ba3.ndarray = arr3
        ref = content.bounded_arrays.add(); ref.SetLink(ba1)
        ref = content.bounded_arrays.add(); ref.SetLink(ba2)
        ref = content.bounded_arrays.add(); ref.SetLink(ba3)
        var1.content = content
        
        self.var = var1
    
    @defer.inlineCallbacks
    def setup_nD_multiple_BA(self, nD, bounded_arrays=10, segment_size=10):
        """
        @param segment_size: The number of values per dimension in this variable
        """
        # Setup the dimension objects
        dim_list = []
        dim_list.append(self.root.AddDimension('dim0', bounded_arrays))
        for i in range(1, nD):
            dim_name = 'Dim%i' % i
            dim_list.append(self.root.AddDimension(dim_name, segment_size))
        var = self.root.AddVariable('var', self.root.DataType.DOUBLE, dim_list)
        content = yield var.Repository.create_object(CDM_ARRAY_STRUC_TYPE)
        
        # Configure the array's bounds
        vals_per_slice = ((segment_size) ** (nD - 1))
        log.debug('Using %i values per ndarray (slice)' % vals_per_slice)
        for i in range(bounded_arrays):
            log.debug('')
            log.debug('')
            log.debug('Creating Bounded array %i...' % i)
            # Create the array structures
            ba = yield content.Repository.create_object(CDM_BOUNDED_ARRAY_TYPE)
            arr = yield ba.Repository.create_object(CDM_F64_ARRAY_TYPE)

            # Break up the first dimension into 10 parts
            # (1 bounded array per part, 10 values per bounded array)
            ba.bounds.add()
            ba.bounds[0].origin = i
            ba.bounds[0].size = 1
            log.debug('Bounds[0]: origin(%i) size(%i)' % (i, bounded_arrays))
            
            # Each dimension of each bounded array is totally inclusive
            # except the first bounded dimension.. it is broken into 10 parts
            for j in range(1, nD):
                ba.bounds.add()
                ba.bounds[j].origin = 0
                ba.bounds[j].size = segment_size
                log.debug('Bounds[0]: origin(%i) size(%i)' % (0, segment_size))
        
            # Insert values into the arrays
            start = vals_per_slice * i
            end = start + vals_per_slice
            log.debug('Inserting values in the range %i to %i' % (start, end))
            arr.value.extend([float(val) for val in range(start, end)])
        
            # Put the pieces together
            ba.ndarray = arr
            ref = content.bounded_arrays.add(); ref.SetLink(ba)

        # Attach the content        
        var.content = content
        self.var = var
    
    @defer.inlineCallbacks
    def test_GetValue_1D_multiple_BA(self):
        yield self.setup_1D_multiple_BA()
        
        for i in range(90):
            val = self.var.GetValue(i)
            self.assertEquals(i, val)
    
    @defer.inlineCallbacks
    def test_GetValue_2D_multiple_BA(self):
        num_dims = 2
        num_arrs = 31
        num_vals = 59
        yield self.setup_nD_multiple_BA(num_dims, num_arrs, num_vals)
        
        
        for i in range(num_arrs):
            for j in range(num_vals):
                val = self.var.GetValue(i, j)

                # Assert that the stored value matches its calculated equal
                self.assertEquals((i * num_vals) + j, val)
    
    @defer.inlineCallbacks
    def test_GetValue_3D_multiple_BA(self):
        num_dims = 3
        num_arrs = 13
        num_vals = 17
        yield self.setup_nD_multiple_BA(num_dims, num_arrs, num_vals)
        
        
        count = 0
        for i in range(num_arrs):
            for j in range(num_vals):
                for k in range(num_vals):
                    val = self.var.GetValue(i, j, k)

                    # Assert that the stored value matches its calculated equal
                    self.assertEquals(count, val)
                    count += 1
    
    @defer.inlineCallbacks
    def test_GetValue_4D_multiple_BA(self):
        num_dims = 4
        num_arrs = 7
        num_vals = 9
        yield self.setup_nD_multiple_BA(num_dims, num_arrs, num_vals)
        
        count = 0
        for i in range(num_arrs):
            for j in range(num_vals):
                for k in range(num_vals):
                    for l in range(num_vals):
                        val = self.var.GetValue(i, j, k, l)
    
                        # Assert that the stored value matches its calculated equal
                        self.assertEquals(count, val)
                        count += 1
    
    
    @defer.inlineCallbacks
    def test_GetValue_5D_multiple_BA(self):
        num_dims = 5
        num_arrs = 3
        num_vals = 7
        yield self.setup_nD_multiple_BA(num_dims, num_arrs, num_vals)
        
        
        count = 0
        for i in range(num_arrs):
            for j in range(num_vals):
                for k in range(num_vals):
                    for l in range(num_vals):
                        for m in range(num_vals):
                            
                            val = self.var.GetValue(i, j, k, l, m)
        
                            # Assert that the stored value matches its calculated equal
                            self.assertEquals(count, val)
                            count += 1
    

    def test_fail_flatten_index(self):
        self.assertRaises(AssertionError, _flatten_index, None, [])
        self.assertRaises(AssertionError, _flatten_index, [], None)
        self.assertRaises(AssertionError, _flatten_index, [1, 2], [1, 2, 3])
        
        
        
        
        
