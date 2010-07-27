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

"""
DM Pub Sub Registry Resource Descriptions
"""

class PublicationResource(StatefulResource):
    """
    A registry object which contai
    """
    #Name - inherited!
    queue = TypedAttribute(str)
    topics = TypedAttribute(list) # List of refs for registered topics
    content_type = TypedAttribute(str)
    
class AOI(DataObject):
    """
    Implement class and comparison methods for AOI!
    """
    
    
class PubSubTopic(InformationResource):
    """
    """
    #Name - inherited
    keywords = TypedAttribute(str)
    aoi = TypedAttribute(AOI)

class SubscriptionResource(StatefulResource):
    """
    """
    #Name - inherited
    queue = TypedAttribute(str)
    topics = TypedAttribute(list)
    period = TypedAttribute(list)
    interval = TypedAttribute(int,0)




"""
DM DataSet Resource Descriptions 
"""
class PerservationServiceResource(StatefulResource): # Is it stateful or information?
    #Name (Logical IRODS Name) - inherited
    datatype = TypedAttribute(str)
    archived_locations = TypedAttribute(list) # List of Preservation Location objects

class PreservationLocation(DataObject):
    location = TypedAttribute(str)

class TypedObject(DataObject):
    """
    """
    
class FloatObject(TypedObject):
    """
    #@Todo convert to use numpy types
    """
    f = TypedAttribute(float)
    
class IntegerObject(TypedObject):
    """
    #@Todo convert to use numpy types
    """
    i = TypedAttribute(int)
    
class StringObject(TypedObject):
    """
    #@Todo convert to use numpy types
    """
    s = TypedAttribute(str)
    

class CDMDatasetResource(InformationResource):
    '''
    @Note <class> must be a type which python can instantiate with eval!
    '''
    #Name - inherited
    groups = TypedAttribute(list)
    preservation_archive = TypedAttribute(PerservationServiceResource)
    
class CDMGroupResource(InformationResource):
    #Name - inherited
    attributes = TypedAttribute(list)
    dimensions = TypedAttribute(list)
    variables = TypedAttribute(list)
    preservation_archive = TypedAttribute(PerservationServiceResource)
    
class CDMAttributeResource(InformationResource):
    #Name - inherited
    value = TypedAttribute(TypedObject)
    preservation_archive = TypedAttribute(PerservationServiceResource)
    archive_attid = TypedAttribute(int,0) # Varid or name?
    
class CDMDimensionResource(InformationResource):
    #Name - inherited
    dim = TypedAttribute(int,0)
    unlimited = TypedAttribute(bool,False)
    shared = TypedAttribute(bool,False)
    is_variable_length = TypedAttribute(bool,False)
    preservation_archive = TypedAttribute(PerservationServiceResource)
    archive_dimid = TypedAttribute(int,0) # Varid or name?
    
class CDMVariableResource(InformationResource):
    """
    """
    #Name - inherited
    attributes = TypedAttribute(list)
    dimensions = TypedAttribute(list)
    type = TypedAttribute(str)
    preservation_archive = TypedAttribute(PerservationServiceResource)
    archive_varid = TypedAttribute(str) # Varid or name?
    
class CDMStructureResource(InformationResource):
    """
    """
    #Name - inherited
    members = TypedAttribute(list)
    

    

    
    