#!/usr/bin/env python

import inspect
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource

from ion.core.cc.container import Id
"""
Container object are used as self describing sendable objects
"""
class SetResourceLCStateContainer(DataObject):
    """
    @ Brief a message object used to set state and
    """
    # Beware of using a class object as a typed attribute!
    lcstate = TypedAttribute(LCState, default=None)
    reference = TypedAttribute(ResourceReference)


class ResourceListContainer(DataObject):
    """
    @ Brief a message object used to pass a list of resource description objects
    """
    resources = TypedAttribute(list, default=None)


class FindResourceContainer(DataObject):
    """
    @ Brief a message object used to find resource description in a registry
    @ note string_comparison_method can be 'regex' or '=='
    """
    description = TypedAttribute(Resource, default=None)
    regex = TypedAttribute(bool, default=True)
    ignore_defaults = TypedAttribute(bool, default=True)
    attnames = TypedAttribute(list)


"""
Resource Description object are used in the OOICI Registries
"""

"""
Define properties of resource types
@note What is the intent with Resource Types?
"""
ResourceTypes = ['generic',
                'unassigned',
                'information',
                'service',
                'stateful',
                'taskable'
                ]

class ResourceType(object):
    """
    @brief Class to control the possible states based on the LCStateNames list
    """

    def __init__(self, type='unassigned'):
        assert type in ResourceTypes
        self._type = type

    def __repr__(self):
        return self._type

    def __eq__(self, other):
        assert isinstance(other, ResourceType)
        return str(self) == str(other)

OOIResourceTypes = dict([('ResourceType', ResourceType)] + [(name, ResourceType(name)) for name in ResourceTypes])

class TypesContainer(dict):
    """
    Class used to set the the possible types
    """

    def __init__(self, d):
        dict.__init__(self, d)
        for k, v in d.items():
            setattr(self, k, v)

OOIResourceTypes = TypesContainer(OOIResourceTypes)

# Classes that do not inherit from DataObject must be explicitly added to the data
# Object Dictionary to be decoded!
DataObject._types.update(OOIResourceTypes)

class ResourceDescription(InformationResource):
    """
    Resource Descriptions are stored in the resource registry.
    They describe resources, resource types and resource attributes
    """
    type = TypedAttribute(ResourceType)
    atts = TypedAttribute(list,[])
    inherits_from = TypedAttribute(list)
    description = TypedAttribute(str)

class AttributeDescription(DataObject):
    name = TypedAttribute(str)
    type = TypedAttribute(str)
    default = TypedAttribute(str)

class ResourceInstance(StatefulResource):
    """
    Resource Instances are stored in the resource registry.
    They describe instances of a resource type
    """
    description = TypedAttribute(ResourceReference)
    owner = TypedAttribute(ResourceReference)
    resource = TypedAttribute(ResourceReference)

class IdentityResource(StatefulResource):
    """
    Identity Resources are stored in the identity registry
    Identity Resources describe the identity of human in the OOICI...
    """
    # These are the fields that we get from the Trust Provider
    #ooi_id = TypedAttribute(str)
    common_name = TypedAttribute(str)
    country = TypedAttribute(str)
    trust_provider = TypedAttribute(str) # this is the trust provider /O (Organization field)
    domain_component = TypedAttribute(str)
    certificate = TypedAttribute(str)
    rsa_private_key = TypedAttribute(str)
    expiration_date = TypedAttribute(str)
    # These are the fields we prompt the user for during registration
    first_name = TypedAttribute(str)
    last_name = TypedAttribute(str)
    phone = TypedAttribute(str)
    fax = TypedAttribute(str)
    email = TypedAttribute(str)
    organization = TypedAttribute(str)
    department = TypedAttribute(str)
    title = TypedAttribute(str)



class ServiceDescription(InformationResource):
    """
    Resource Descriptions are stored in the resource registry.
    They describe resources, resource types and resource attributes
    """
    interface = TypedAttribute(list)
    module = TypedAttribute(str)
    version = TypedAttribute(str)
    #spawnargs = TypedAttribute(dict,{})
    description = TypedAttribute(str)
    class_name = TypedAttribute(str)


class ServiceMethodInterface(DataObject):
    name = TypedAttribute(str)
    description = TypedAttribute(str)
    arguments = TypedAttribute(str)



class ServiceInstance(StatefulResource):
    """
    Resource Instances are stored in the resource registry.
    They describe instances of a resource type
    Attribute names are taken from ProcessDesc class ?
    """
    description = TypedAttribute(ResourceReference)
    #proc_module = TypedAttribute(str)
    proc_node = TypedAttribute(str)
    proc_id = TypedAttribute(Id, Id(None))
    proc_name = TypedAttribute(str)
    spawn_args = TypedAttribute(dict)
    proc_state = TypedAttribute(str)
    sup_process = TypedAttribute(ResourceReference)
    #proc_mod_obj = TypedAttribute(str)
    #proc_class = TypedAttribute(str)

DataObject._types['Id']=Id

class AgentDescription(InformationResource):
    """
    Agent Descriptions are stored in the agent registry.
    They describe an agent, its interface and attributes
    """
    interface = TypedAttribute(list)
    module = TypedAttribute(str)
    #version = TypedAttribute(str)
    #spawnargs = TypedAttribute(dict,{})
    description = TypedAttribute(str)
    class_name = TypedAttribute(str)

class AgentMethodInterface(StatefulResource):
    description = TypedAttribute(str)
    arguments = TypedAttribute(str)

class AgentInstance(StatefulResource):
    """
    Agent Instances are stored in the agent registry.
    They describe instances of an agent and reference its description and
    the subject of the agent.
    """
    description = TypedAttribute(ResourceReference)
    #owner = TypedAttribute(ResourceReference)
    spawnargs = TypedAttribute(str)
    proc_id = TypedAttribute(str)
    proc_name = TypedAttribute(str)
    proc_state = TypedAttribute(str)
    subject = TypedAttribute(ResourceReference)
    
    
class ExchangeSpace(InformationResource):
    """
    ExchangeSpaces provide a hierarchy or grouping of ExchangeNames.
    They offer a level of granularity for governance and policy enforcement.
    Eventually they will play a part in the strategy of segregating exchange
    traffic and permissions.
    """
    name = TypedAttribute(str)
    description = TypedAttribute(str)
    exchangenames = TypedAttribute(list) # 1-N ExchangeName below


class ExchangeName(InformationResource):
    """
    ExchangeName binds a name (string) to an AMQPMapping and a 
    HardwareMapping.  
    """
    name = TypedAttribute(str)
    description = TypedAttribute(str)
    amqpmapping = TypedAttribute(StatefulResource) # 1-1 ExchangeMapping below
    hardwaremapping = TypedAttribute(StatefulResource) # 1-1 HardwareMapping below
    
    
class AMQPMapping(StatefulResource):
    """
    ExchangeMapping carries information about an exchange:  
    type (fanout, direct, etc), routing keys, queues, and so forth.
    """
    name = TypedAttribute(str)           # unnecessary?         
    description = TypedAttribute(str)    # unnecessary?
    routing_key = TypedAttribute(str)
    delivery_mode = TypedAttribute(str)  # transient | persistent
    exchange_type = TypedAttribute(str)  # direct | topic | fanout | headers
    durable = TypedAttribute(bool)
    auto_delete = TypedAttribute(bool)
    auto_declare = TypedAttribute(bool)
    
    
class HardwareMapping(StatefulResource):
    """
    TODO:  Flesh this out as more is discovered about Solace routing 
    and/or other hardware routing solutions.
    """
    name = TypedAttribute(str)           # unnecessary?         
    description = TypedAttribute(str)    # unnecessary?

    
class BrokerCredentials(StatefulResource):
    """
    BrokerCredentials carries all the necessary information to authenticate
    and connect to an AMQP broker.
    """
    name = TypedAttribute(str)         # unnecessary?         
    description = TypedAttribute(str)  # unnecessary?
    hostname = TypedAttribute(str)
    port = TypedAttribute(int)         
    connect_timeout = TypedAttribute(int)
    ssl = TypedAttribute(bool)
    vhost = TypedAttribute(str)
    insist = TypedAttribute(bool)
    userid = TypedAttribute(str)
    password = TypedAttribute(str)

    
class BrokerFederation(StatefulResource):
    """
    TODO:  Flesh this out as federation becomes more of a reality.
    """
    