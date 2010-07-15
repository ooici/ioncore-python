#!/usr/bin/env python


from ion.data.dataobject import DataObject, Resource, TypedAttribute, LCState, LCStates, ResourceReference, InformationResource, StatefulResource

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
    
    
"""
Resource Description object are used in the OOICI Registries
"""

"""
Define properties of resource types
"""
ResourceTypes = ['generic',
                'unassigned',
                'information',
                'service',
                'stateful'
                ]

class ResourceType(object):
    """
    @Brief Class to control the possible states based on the LCStateNames list
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

class AttributeDescription(DataObject):
    name = TypedAttribute(str)
    type = TypedAttribute(str)
    default = TypedAttribute(str)
    resource_description = \
    'This is a resource used in the resource registry.\n\
    It provides for the description of resource attributes'


class ResourceDescription(InformationResource):
    type = TypedAttribute(ResourceType)
    atts = TypedAttribute(list)
    inherits_from = TypedAttribute(ResourceReference)
    description = TypedAttribute(str)
    resource_description = \
    'This is a resource used in the resource registry.\n\
    It describes resource types'
    
