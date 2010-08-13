#!/usr/bin/env python

"""
@file ion/resources/dm_resource_descriptions.py
@author David Stuebe
@brief Resource descriptions for DM services and resources. Includes some
container objects used in messaging.
"""

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
    notification = TypedAttribute(str)
    timestamp = TypedAttribute(float)
    
    
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

    #owner = TypedAttribute(ResourceReference) # Don't worry about owner yet
    
    # hack for now to allow naming one-three more topic descriptions
    topic1 = TypedAttribute(PubSubTopicResource)
    topic2 = TypedAttribute(PubSubTopicResource) 
    topic3 = TypedAttribute(PubSubTopicResource)
    
    workflow = TypedAttribute(dict)
    '''
    Only specify who you attach to - not who you produce to - consistent with pubsub model!
    <consumer name>:{'module':'path.to.module','cosumeclass':'<ConsumerClassName>',\
        'attach':<topicX> or <consumer name> or <list of consumers and topics>,\
        'Process Parameters':{<conumser property keyword arg>: <property value>},\
    '''

    #Used internally
    #current_topics = TypedAttribute(list) # List of Topic Resource References
    #consumer_procids = TypedAttribute(dict) # list of child consumer ids - need a process registry
    queues = TypedAttribute(list) # list of queue objects
    consumer_args = TypedAttribute(dict)


"""
Archive Registry Resources
"""
class ArchiveResource(StatefulResource): # Is it stateful or information?
    #Name (Logical IRODS Name) - inherited
    datatype = TypedAttribute(str)
    cache_policy = TypedAttribute(str)
    backup_policy = TypedAttribute(str)
    locations = TypedAttribute(list) # List of Archive Location objects

class ArchiveLocation(DataObject):
    name = TypedAttribute(str)
    location = TypedAttribute(str)

"""
DM Data Registry Resources
Preliminary!
"""

class AttributeData(DataObject):
    """
    """
    
class FloatAttribute(AttributeData):
    """
    #@Todo convert to use numpy types
    """
    f = TypedAttribute(float)
    
class IntegerAttribute(AttributeData):
    """
    #@Todo convert to use numpy types
    """
    i = TypedAttribute(int)
    
class StringAttribute(AttributeData):
    """
    #@Todo convert to use numpy types
    """
    s = TypedAttribute(str)
    

class DMDataResource(InformationResource):
    '''
    @Note <class> must be a type which python can instantiate with eval!
    '''
    #Name - inherited
    groups = TypedAttribute(list)
    archive = TypedAttribute(ArchiveResource)
    
class DMGroupData(InformationResource):
    #Name - inherited
    attributes = TypedAttribute(list)
    dimensions = TypedAttribute(list)
    variables = TypedAttribute(list)
    archive = TypedAttribute(ArchiveResource)
    
class DMAttributeData(InformationResource):
    #Name - inherited
    value = TypedAttribute(AttributeData)
    archive = TypedAttribute(ArchiveResource)
    archive_attid = TypedAttribute(int,0) # Varid or name?
    
class DMDimensionData(InformationResource):
    #Name - inherited
    dim = TypedAttribute(int,0)
    unlimited = TypedAttribute(bool,False)
    shared = TypedAttribute(bool,False)
    is_variable_length = TypedAttribute(bool,False)
    archive = TypedAttribute(ArchiveResource)
    archive_dimid = TypedAttribute(int,0) # Varid or name?
    
class DMVariableData(InformationResource):
    """
    """
    #Name - inherited
    attributes = TypedAttribute(list)
    dimensions = TypedAttribute(list)
    type = TypedAttribute(str)
    archive = TypedAttribute(ArchiveResource)
    archive_varid = TypedAttribute(str) # Varid or name?
    
class DMStructureData(InformationResource):
    """
    """
    #Name - inherited
    members = TypedAttribute(list)
    
    




    
    