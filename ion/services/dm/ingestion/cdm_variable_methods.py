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

    # Check the rank outside the for loop
    # check to make sure args are integers!

    value = None

    for ba in self.bounded_arrays:

        for index, bounds in zip(args, ba.bounds):

            if bounds.origin > index or index >= bounds.origin + bounds.size :
                break

        else:

            # Get the ndarray of interest and extract the value!

            value = 3 # magic happens here to get the value!
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



