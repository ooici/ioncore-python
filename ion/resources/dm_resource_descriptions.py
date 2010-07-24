#!/usr/bin/env python

from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource

"""
class EXAMPLE_RESOURCE(ResourceDescription):
    '''
    @Note <class> must be a type which python can instantiate with eval!
    '''
    att1 = TypedAttribute(<class>, default=None)
    att2 = TypedAttribute(<class>)
"""




class PerservationServiceResource(StatefulResource): # Is it stateful or information?
    logical_name = TypedAttribute(str)
    datatype = TypedAttribute(str)
    archived_locations = TypedAttribute(list) # List of PreservationLocation objects

class PreservationLocation(DataObject):
    location = TypedAttribute(str)


class CDMDatasetResource(InformationResource):
    '''
    @Note <class> must be a type which python can instantiate with eval!
    '''
    groups = TypedAttribute(list)
    preservation_archive = TypedAttribute(PerservationServiceResource)
    
class CDMGroupResource(InformationResource):
    attributes = TypedAttribute(list)
    dimensions = TypedAttribute(list)
    variables = TypedAttribute(list)
    
class TypedObject(DataObject):
    """
    """
    
class CDMAttributeResource(InformationResource):
    attname = TypedAttribute(str)
    value = TypedAttribute(TypedObject)
    

    
class FloatObject(TypedObject):
    f = TypedAttribute(float)
    
class IntegerObject(TypedObject):
    i = TypedAttribute(int)
    
class StringObject(TypedObject):
    s = TypedAttribute(str)
    
    
    
class CDMDimensionResource(InformationResource):
    dim = TypedAttribute(int,0)
    dimname = TypedAttribute(str)
    unlimited = TypedAttribute(bool,False)
    
class CDMVariableResource(InformationResource):
    """
    """
class CDMStructureResource(InformationResource):
    """
    """
    
    