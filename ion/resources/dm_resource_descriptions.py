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
This is a first go - but worth a look!
"""

class PublicationResource(StatefulResource):
    """
    A registry object which contains information about publishers
    """
    #Name - inherited!
    topics = TypedAttribute(list) # List of Topic Resource References
    content_type = TypedAttribute(str)
    queue = TypedAttribute(str)

class AOI(DataObject):
    """
    Implement class and comparison methods for AOI!
    """  
    
class PubSubTopic(InformationResource):
    """
    A topic definition which can be stored in the registry
    Contains a Name, a Keyword, an Exchange Queue, and an AOI
    """
    #Name - inherited
    queue = TypedAttribute(str)
    keywords = TypedAttribute(str)
    aoi = TypedAttribute(AOI)

class SubscriptionResource(StatefulResource):
    """
    Informaiton about a subscriber
    """
    #Name - inherited
    topics = TypedAttribute(list) # List of Topic Resource References
    period = TypedAttribute(list)
    interval = TypedAttribute(int,0)

"""
DM DataSet Resource Descriptions
Preliminary!
"""
class PerservationServiceResource(StatefulResource): # Is it stateful or information?
    #Name (Logical IRODS Name) - inherited
    datatype = TypedAttribute(str)
    archived_locations = TypedAttribute(list) # List of Preservation Location objects

class PreservationLocation(DataObject):
    location = TypedAttribute(str)

class TypedData(DataObject):
    """
    """
    
class FloatData(TypedData):
    """
    #@Todo convert to use numpy types
    """
    f = TypedAttribute(float)
    
class IntegerData(TypedData):
    """
    #@Todo convert to use numpy types
    """
    i = TypedAttribute(int)
    
class StringData(TypedData):
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
    value = TypedAttribute(TypedData)
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
    

    

    
    