#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/cdm_attribute_methods.py
@author David Stuebe

@Brief Methods for merging CDM Dataset attributes

Based on Porter - Duff Alpha Composite rules
"""


def MergeAttSrc(self, attname, src):
    """
    The Source overwrites the destination

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """

    # - Check if MyId of src att is the same as MyId of dst att - a shortcut for equality!


    return None


def MergeAttDst(self, attname, src):
    """
    The Destination is unchanged - ignore the source - a NoOp!

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """

    return None

def MergeAttGreater(self, attname, src):
    """
    Keep the greater of the two attribute values

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """

    # - Check if MyId of src att is the same as MyId of dst att - a shortcut for equality!


    return None

def MergeAttLesser(self, attname, src):
    """
    Keep the lesser of the two attribute values

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """

    # - Check if MyId of src att is the same as MyId of dst att - a shortcut for equality!


    return None


def MergeAttDstOver(self, attname, src):
    """
    Merge the Destination over the Source. Use case: Global Att - history
    Deduplicate the list of attrs and append the dest.

    Add more examples!

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """

    # - Check if MyId of src att is the same as MyId of dst att - a shortcut for equality!


    return None


