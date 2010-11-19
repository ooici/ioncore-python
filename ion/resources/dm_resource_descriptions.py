#!/usr/bin/env python

"""
@file ion/resources/dm_resource_descriptions.py
@author David Stuebe
@author Matt Rodriguez
@brief Resource descriptions for DM services and resources. Includes some
container objects used in messaging.
"""

from ion.data.dataobject import DataObject, TypedAttribute, ResourceReference, InformationResource, StatefulResource

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
        inst.publisher = publisher_proc.id.full

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
    @brief The exchange message Queue is really an exchange registry object
    @todo move to the exchange registry and use it properly!
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
        @brief Create a topic resource and set parameters
        @param name is the topic name
        @param keywords are comma seperated key words for the topic
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

    # hack for now to allow naming one-three more topic descriptions used to find topics that are subscribed to!
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
#class ArchiveLocation(DataObject):
#    name = TypedAttribute(str,'')
#    location = TypedAttribute(str)

class ArchiveResource(StatefulResource): # Is it stateful or information?
    #Name (Logical IRODS Name) - inherited
    datatype = TypedAttribute(str,'dap')
    cache_policy = TypedAttribute(str,'none')
    backup_policy = TypedAttribute(str,'none')
    topic = TypedAttribute(ResourceReference)
    #locations = TypedAttribute(list,[]) # List of Archive Location objects
    dmdataresource = TypedAttribute(ResourceReference)

"""
DM Preservation Management Resources
"""
class Cache(StatefulResource):
    """
    persistentarchive: The name of the Persistent Archive
    partition: The name of the partition, this could be a Column Family
    in Cassandra or a collection in IRODS.
    PartitionType: The kind of partition, such as Column Family or Collection. 
    """
    persistentarchive = TypedAttribute(str)
    partition = TypedAttribute(str)
    partitiontype = TypedAttribute(str)

class PersistentArchive(StatefulResource):
    """
    This resource holds common attributes for all of the Persistent Archives.
    
    datatype: The kind of persistent archive, for example Cassandra or IRODS.
    
    """
    datatype = TypedAttribute(str)

class CassandraPersistentArchive(StatefulResource):
    """
    hosts: The hosts in the Cassandra cluster. A list of strings.
    keyspace: The name of the Keyspace
    columnfamily: The name of the Column Family
    """
    hosts = TypedAttribute(list)
    keyspace = TypedAttribute(str)
    columnfamily = TypedAttribute(str)
    

class IRODSPersistentArchive(StatefulResource):
    """
    host: A host of the IRODS instance.
    ICAT: The name of the ICAT database. Information needed to 
    zone: Information about the IRODS zone.
    auth: Authentication information to log into the IRODS instance.
    """
    host = TypedAttribute(str)
    ICAT = TypedAttribute(str)
    zone = TypedAttribute(str)
    auth = TypedAttribute(str)
"""
DM Data Registry Resources
Preliminary!
"""

class AttributeData(DataObject):
    """
    """

class FloatAttribute(AttributeData):
    """
    #@todo convert to use numpy types
    """
    f = TypedAttribute(float)

class IntegerAttribute(AttributeData):
    """
    #@todo convert to use numpy types
    """
    i = TypedAttribute(int)

class StringAttribute(AttributeData):
    """
    #@todo convert to use numpy types
    """
    s = TypedAttribute(str)


class DMDataResource(InformationResource):
    '''
    @note <class> must be a type which python can instantiate with eval!
    '''
    #Name - inherited
    metadata = TypedAttribute(ResourceReference)
    packetsreceived = TypedAttribute(int,0)
    packetspersisted = TypedAttribute(int,0)
    input_archive = TypedAttribute(ResourceReference)
    input_topic = TypedAttribute(ResourceReference)
    ingested_archive = TypedAttribute(ResourceReference)
    ingested_topic = TypedAttribute(ResourceReference)


class CDMResource(InformationResource):
    '''
    A resource class to describe Unidata Common data model data
    '''
    groups = TypedAttribute(list)


class DMGroupData(CDMResource):
    #Name - inherited
    attributes = TypedAttribute(list)
    dimensions = TypedAttribute(list)
    variables = TypedAttribute(list)
    dmdataresource = TypedAttribute(ResourceReference)
    archive_grpid = TypedAttribute(int,0)

class DMAttributeData(CDMResource):
    #Name - inherited
    value = TypedAttribute(AttributeData)
    archive_attid = TypedAttribute(int,0) # Varid or name?
    dmdataresource = TypedAttribute(ResourceReference)

class DMDimensionData(CDMResource):
    #Name - inherited
    dim = TypedAttribute(int,0)
    unlimited = TypedAttribute(bool,False)
    shared = TypedAttribute(bool,False)
    is_variable_length = TypedAttribute(bool,False)
    dmdataresource = TypedAttribute(ResourceReference)
    archive_dimid = TypedAttribute(int,0) # Varid or name?

class DMVariableData(CDMResource):
    """
    """
    #Name - inherited
    attributes = TypedAttribute(list)
    dimensions = TypedAttribute(list)
    type = TypedAttribute(str)
    dmdataresource = TypedAttribute(ResourceReference)
    archive_varid = TypedAttribute(str) # Varid or name?

class DMStructureData(CDMResource):
    """
    """
    #Name - inherited
    members = TypedAttribute(list)
    # What?


"""
DM Ingestion data stream object
"""
class IngestionStreamResource(StatefulResource):
    # name - inherited from StatefulResource
    input_topic_ref = TypedAttribute(ResourceReference)
    #input_format = TypedAttribute(str)
    ingested_topic_ref = TypedAttribute(ResourceReference)
    #ingested_format = TypedAttribute(str)
    persisting_input = TypedAttribute(bool)
    persisteing_ingested = TypedAttribute(bool)
    ingesting = TypedAttribute(bool)
    dataregistry = TypedAttribute(ResourceReference)
