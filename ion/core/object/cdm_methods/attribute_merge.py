#!/usr/bin/env python

"""
@file ion/services/dm/ingestion/cdm_attribute_methods.py
@author David Stuebe

@Brief Methods for merging CDM Dataset attributes

Loosely based on Porter-Duff Image Compositing rules
"""

# Get the object decorator used on wrapper methods!
from ion.core.object.object_utils import _gpb_source

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import time, calendar
from ion.core.object.gpb_wrapper import OOIObjectError
from ion.util import procutils as pu

@_gpb_source
def MergeAttSrc(self, attname, src):
    """
    The Source overwrites the destination

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination
    
    """
    # @raise OOIObjectError: When the named attribute in src and the named attribute in dst have mismatched types
    # @note: Check if MyId of src att is the same as MyId of dst att - a shortcut for equality!
    # @todo: Type checking -- ensure src and dest are groups or variables
    
    
    # Grap the attribute objects from both sources
    (src_att, dst_att) = _get_attribs(src, self, attname)
    
    if src_att is None:
        log.info('Source attribute is None and cannot be merged into Dest.  Attributes will remain unchanged')
        return None

    if dst_att is None:
        # Add the attribute anew
        log.debug('Adding new attribute into destination')
        self.AddAttribute(attname, src_att.GetDataType(), src_att.GetValues())
        return None
    
    if src_att.MyId == dst_att.MyId:
        log.debug('Src and Dst attributes are the same.  Attributes will remain unchanged')
        return None


    # Replace the existing attribute
    # @todo: Should we manually remove the old attribute first?  If we do
    #        we need not be concerned with the matching of attribute types
#        if not dst_att.IsSameType(src_att):
#            raise OOIObjectError('Attributes have mismatched types according to "Attribute.IsSameType(...)"')
    log.debug('Copying src attribute into destination')
    self.SetAttribute(attname, src_att.GetValues(), src_att.GetDataType())
    

    return None

@_gpb_source
def MergeAttDst(self, attname, src):
    """
    The Destination is unchanged - ignore the source - a NoOp!

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """
    # NO-OP
    return None

@_gpb_source
def MergeAttGreater(self, attname, src):
    """
    Keep the greater of the two attribute values

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """
    # @note: Check if MyId of src att is the same as MyId of dst att - a shortcut for equality!
    # @todo: Type checking -- ensure src and dest are groups or variables
    

    # Grap the attribute objects from both sources
    (src_att, dst_att) = _get_attribs(src, self, attname)
    
    if src_att is None:
        log.info('Source attribute is None and cannot be merged into Dest.  Attributes will remain unchanged')
        return None
    
    if dst_att is None:
        log.info('Dest attribute is None and will be disregarded.  Source will replace Dest')
        self.AddAttribute(attname, src_att.GetDataType(), src_att.GetValues())
        return None
        
    # @todo: Ensure the length of the attribute list is exactly ONE
    
    
    src_val = self._GetNumericValue(src_att.GetDataType(), src_att.GetValue())
    dst_val = self._GetNumericValue(dst_att.GetDataType(), dst_att.GetValue())
    
    if pu.isnan(src_val) or pu.isnan(dst_val):
        raise ValueError('Cannot merge valid attributes with NaN values for attribute "%s". SRC: %s.   DST: %s' % (attname, str(src_val), str(dst_val)))
    
    if src_val > dst_val:
        self.SetAttribute(attname, src_att.GetValues(), src_att.GetDataType())
    elif dst_val > src_val:
        pass # NO-OP (dst_val is already set in the destination)
    else:
        log.info('Src and Dst attribute values are the same.  Attributes will remain unchanged')


    return None

@_gpb_source
def MergeAttLesser(self, attname, src):
    """
    Keep the lesser of the two attribute values

    @param self - the destination Variable or Group to be modified
    @param attname - the name of the attribute to merge
    @param src - the source Variable or Group to be applied to the destination

    """
    # @note: Check if MyId of src att is the same as MyId of dst att - a shortcut for equality!
    # @todo: Type checking -- ensure src and dest are groups or variables
    

    # Grap the attribute objects from both sources
    (src_att, dst_att) = _get_attribs(src, self, attname)
    
    if src_att is None:
        log.info('Source attribute is None and cannot be merged into Dest.  Attributes will remain unchanged')
        return None
    
    if dst_att is None:
        log.info('Dest attribute is None and will be disregarded.  Source will replace Dest')
        self.AddAttribute(attname, src_att.GetDataType(), src_att.GetValues())
        return None
        
    # @todo: Ensure the length of the attribute list is exactly ONE
    
    
    src_val = self._GetNumericValue(src_att.GetDataType(), src_att.GetValue())
    dst_val = self._GetNumericValue(dst_att.GetDataType(), dst_att.GetValue())
    
    
    if pu.isnan(src_val) or pu.isnan(dst_val):
        raise ValueError('Cannot merge valid attributes with NaN values for attribute "%s". SRC: %s.   DST: %s' % (attname, str(src_val), str(dst_val)))
        
    if src_val < dst_val:
        self.SetAttribute(attname, src_att.GetValues(), src_att.GetDataType())
    elif dst_val < src_val:
        pass # NO-OP (dst_val is already set in the destination)
    else:
        log.info('Src and Dst attribute values are the same.  Attributes will remain unchanged')


    return None

@_gpb_source
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


def _get_attribs(src, dst, attname):
    src_att = None
    dst_att = None
    if src.HasAttribute(attname):
        try:
            src_att = src.FindAttributeByName(attname)
        except OOIObjectError, ex:
            log.warn("Error finding attribute by name.  Cause: %s" % str(ex))

    if dst.HasAttribute(attname):
        try:
            dst_att = dst.FindAttributeByName(attname)
        except OOIObjectError, ex:
            log.warn("Error finding attribute by name.  Cause: %s" % str(ex))
    
    return (src_att, dst_att)

@_gpb_source
def _GetNumericValue(self, data_type, value):
    
    def _norm_string(val):
        result = 0
        if ':' in val:
            result = calendar.timegm(time.strptime(val, '%Y-%m-%dT%H:%M:%SZ'))
        elif '.' in val:
            result = float(val)
        else:
            result = int(val) # int() method will  upcast to long if necessary!
        return result
        
    _get_numeric_value = {
                            self.DataType.BYTE        : lambda val: val,
                            self.DataType.SHORT       : lambda val: val,
                            self.DataType.INT         : lambda val: val,
                            self.DataType.LONG        : lambda val: val,
                            self.DataType.FLOAT       : lambda val: val,
                            self.DataType.DOUBLE      : lambda val: val,
                            self.DataType.CHAR        : lambda val: ord(val),
                            self.DataType.STRING      : lambda val: _norm_string(val),
                            # self.DataType.STRUCTURE -- recursive merge not supported
                            # self.DataType.SEQUENCE  -- recursive merge not supported
                            self.DataType.ENUM        : lambda val: int(val) 
                            # self.DataType.OPAQUE
                      }
    return _get_numeric_value[data_type](value)
    

'''

#----------------------------#
# Application Startup
#----------------------------#
:: bash ::
bin/twistd -n cc -h localhost -a sysname=cdmtest,register=demodata res/apps/resource.app


#------------------------------------#
# Prepare Dataset Groups for Testing
#------------------------------------#
from datetime import datetime
from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_TRAJ_DATASET_ID, SAMPLE_PROFILE_DATASET_ID, SAMPLE_TRAJ_DATA_SOURCE_ID, SAMPLE_PROFILE_DATA_SOURCE_ID
from ion.services.coi.resource_registry.resource_client import ResourceClient, ResourceInstance
rc = ResourceClient(proc=sup)
ds_deferred = rc.get_instance(SAMPLE_PROFILE_DATASET_ID)

ds = ds_deferred.result
group1 = ds.root_group
title1 = group1.FindAttributeByName('title')
group2 = group1.AddGroup('new_group')
title2 = group2.AddAttribute('title', group2.DataType.STRING, '!! Brand New Title !!')
min_lat1 = group1.FindAttributeByName('ion_geospatial_lat_min')
min_lat2 = group2.AddAttribute('ion_geospatial_lat_min', group2.DataType.DOUBLE, 45.352)
time_start1 = group1.FindAttributeByName('ion_time_coverage_end')
time_start2 = group2.AddAttribute('ion_time_coverage_end', group2.DataType.STRING, datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'))

min_lat1.IsSameType(min_lat2)
title1.IsSameType(title2)

title1.GetValue()
title2.GetValue()


#-----------------------------#
# Start testing merge methods
#-----------------------------#
from ion.services.dm.ingestion.cdm_attribute_methods import *

group1.FindAttributeByName('ion_time_coverage_end').GetValue()
group2.FindAttributeByName('ion_time_coverage_end').GetValue()
MergeAttGreater(group1, 'ion_time_coverage_end', group2)
group1.FindAttributeByName('ion_time_coverage_end').GetValue()
group2.FindAttributeByName('ion_time_coverage_end').GetValue()


group2.HasAttribute('blamo')
MergeAttDst(group2, 'blamo', group1)
group2.HasAttribute('blamo')

group2.HasAttribute('history')
MergeAttSrc(group2, 'history', group1)
group2.FindAttributeByName('history').GetValues()




'''
