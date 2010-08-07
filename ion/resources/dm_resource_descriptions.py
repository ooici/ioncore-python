#!/usr/bin/env python

from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource, create_unique_identity


"""
DM Pub Sub Registry Resource Descriptions
"""

class PublisherResource(StatefulResource):
    """
    A registry object which contains information about publishers
    """
    #Name - inherited!    
    publisher= TypedAttribute(str) #The identity of the publisher
    topics = TypedAttribute(list) # List of Topic Resource References
    content_type = TypedAttribute(str) #What types are there?

    @classmethod
    def create(cls, name, publisher_proc, topics, content_type):
        """
        """
        inst = cls.create_new_resource()
        
        inst.name = name
        inst.publisher = publisher_proc.receiver.spawned.id.full
        
        if not hasattr(topics, '__iter__'):
            topics = [topics]
        
        for topic in topics:
            inst.topics.append(topic.reference(head=True))

        inst.content_type = content_type

        return inst


"""
Pub Sub Messaging objects!
"""    
class DataMessageObject(DataObject):
    """
    Base Class for Data PubSub Message Objects
    """

class DAPMessageObject(DataMessageObject):
    """Container object for messaging DAP data"""
    das = TypedAttribute(str)
    dds = TypedAttribute(str)
    dods = TypedAttribute(str)

class StringMessageObject(DataMessageObject):
    """Container object for messaging STRING data"""
    data = TypedAttribute(str)
    
class DictionaryMessageObject(DataMessageObject):
    """Container object for messaging DICTIONARY data"""
    data = TypedAttribute(dict)
    
    
class Publication(DataObject):
    """
    A container message for things published
    """
    topic_ref = TypedAttribute(ResourceReference) # The registered reference to a topic
    data = TypedAttribute(DataMessageObject) # Any Data Object!
    publisher = TypedAttribute(str) # The identity of the publisher


class AOI(DataObject):
    """
    Implement class and comparison methods for AOI!
    """  
    
class Queue(DataObject):
    '''
    @Brief The exchange message Queue is really an exchange registry object
    @TODO move to the exchange registry and use it properly!
    '''
    type = TypedAttribute(str)
    name = TypedAttribute(str)
    args = TypedAttribute(dict)
    
    
class PubSubTopicResource(InformationResource):
    """
    A topic definition which can be stored in the registry
    Contains a Name, a Keyword, an Exchange Queue, and an AOI
    """
    #name - inherited, a handle for the topic
    
    queue = TypedAttribute(Queue)
    keywords = TypedAttribute(str)
    aoi = TypedAttribute(AOI)    
    
    @classmethod
    def create(cls,name, keywords,aoi=None):
        """
        """
        inst = cls()
        inst.name = name
        inst.keywords = keywords
        if aoi:
            inst.aoi = aoi
        return inst


class SubscriptionResource(StatefulResource):
    """
    Informaiton about a subscriber
    """
    #Name - inherited
    # Subscription is not to a topic but to the Exchange queue where the filtered
    # messages arive from a topic!
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
    
    

    
    