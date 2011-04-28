#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/cdm_variable_methods.py
@author David Stuebe

@Brief Methods for merging and access to CDM Dataset variables

Based on Porter - Duff Alpha Composite rules
"""


def GetValue(self, *args):
    """
    @Brief Get a value from an array structure by its indices
    @param self - a cdm array structure object
    @param args - a list of integer indices for the value to extract

    usage for a 3Dimensional variable:
    as.getValue(1,3,9)
    """
    
    # @todo: Need tests for multidim arrays
    # @todo: Check the rank outside the for loop
    # @todo: Check to make sure args are integers!

    value = None
    
    for ba in self.content.bounded_arrays:
    

        for index, bounds in zip(args, ba.bounds):
            if bounds.origin > index or index >= bounds.origin + bounds.size :
                break

        else:
            # We now have the the ndarray of interest..  extract the value!

            # Create a list of this bounded_array's sizes and use origin to determine
            # the given indices position in the ndarray
            indices = []
            shape = []
            for index, bounds in zip(args, ba.bounds):
                  indices.append(index - bounds.origin)  
                  shape.append(bounds.size)  
            
            # Find the flattened index (make sure to apply the origin values as an offset!)
            flattened_index = _flatten_index(indices, shape)
            
            # Grab the value from the ndarray
            value = ba.ndarray.value[flattened_index]
            break


    return value



def GetIntersectingBoundedArrays(self, bounded_array):
    """
    @brief get the SHA1 id of the bounded arrays which intersect the give coverage.
    @param self - a cdm array structure object
    @param bounded_array - a bounded array which specifies an index space coverage of interest

    usage for a 3Dimensional variable:
    as.getValue(1,3,9)
    """

    # Get the MyId attribute of the bounded arrays that intersect - that will be the sha1 name for that BA...

    sha1_list = []
    return sha1_list


def _flatten_index(indices, shape):
    """
    Uses the given indices representing a position in a multidimensional context to determine the
    equivalent position in a flattened 1D array representation of that same nD context.  The
    cardinality (also size) of each dimension in multidimensional space is given by "shape".
    Note: This means that both "indices" and "shape" must be lists with the same rank (aka length.)
    """
    assert(isinstance(indices, list))
    assert(isinstance(shape, list))
    assert(len(indices) == len(shape))
    
    result = 0
    for i in range(len(indices)):
        offset = 1
        for j in range(i+1, len(shape)):
            offset *= shape[j]
        result += indices[i] * offset
    
    return result



"""
#---------------------------
# Test a 1D dataset
#---------------------------
from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_DLY_DISCHARGE_DATASET_ID
from ion.services.coi.resource_registry.resource_client import ResourceClient
rc = ResourceClient()
ds_d = rc.get_instance(SAMPLE_DLY_DISCHARGE_DATASET_ID)

ds = ds_d.result.ResourceObject
root = ds.root_group
flow = root.FindVariableByName('streamflow')


--------------------------------------------------------------------

from ion.services.dm.ingestion.cdm_variable_methods import GetValue
for i in range(flow.content.bounded_arrays[0].bounds[0].size):
    print GetValue(flow, i)


#---------------------------
# Test a 3D/4D dataset
#---------------------------
from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_HYCOM_DATASET_ID
from ion.services.coi.resource_registry.resource_client import ResourceClient
rc = ResourceClient()
ds_headers_d = rc.get_instance(SAMPLE_HYCOM_DATASET_ID)

ds_slim = ds_headers_d.result.ResourceObject
ds_d = ds_slim.Repository.checkout('master', excluded_types=[])

ds = ds_d.result.resource_object
root = ds.root_group
ssh = root.FindVariableByName('ssh')
for dim in ssh.shape:
    print str(dim.name)


#--------------------------------------------------------------------

from ion.services.dm.ingestion.cdm_variable_methods import GetValue
count = 1
for bounds in ssh.content.bounded_arrays[0].bounds:
    count *= bounds.size 

# All values iterated by bounds
for i in range(ssh.content.bounded_arrays[0].bounds[0].size):
    for j in range(ssh.content.bounded_arrays[0].bounds[1].size):
        for k in range(ssh.content.bounded_arrays[0].bounds[2].size):
            print '(%03i,%03i,%03i) = %s' % (i, j, k, str(ssh.GetValue(i, j, k)))

# List a specific range of Sea Surface Height Values
i = 0
k = 174
for j in range(ssh.content.bounded_arrays[0].bounds[1].size):
    print '(%03i,%03i,%03i) = %s' % (i, j, k, str(ssh.GetValue(i, j, k)))

# List a vertical column of values for temperature
temp = root.FindVariableByName('layer_temperature')
depth = root.FindDimensionByName('Layer')
i = 0
k = 100
l = 200
for j in range(depth.length):
    print '(%03i,%03i,%03i,%03i) = %s' % (i, j, k, l, str(temp.GetValue(i, j, k, l)))



"""
