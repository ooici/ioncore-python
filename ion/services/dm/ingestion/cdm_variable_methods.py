#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/cdm_variable_methods.py
@author David Stuebe

@Brief Methods for merging and access to CDM Dataset variables

Based on Porter - Duff Alpha Composite rules
"""

from ion.services.coi.resource_registry.resource_client import ResourceClient

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
    assert(isinstance(indices, list))
    assert(isinstance(indices, list))
    assert(len(indices) == len(shape))
    
    result = 0
    for i in range(len(indices)):
        offset = 1
        for j in range(i+1, len(shape)):
            offset *= shape[j]
        result += indices[i] * offset
    
    return result



"""

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




"""
