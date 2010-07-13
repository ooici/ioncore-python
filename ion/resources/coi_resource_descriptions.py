#!/usr/bin/env python


from ion.data.dataobject import DataObject, ResourceDescription, TypedAttribute, LCState, LCStates, ResourceReference

class SetResourceLCStateContainer(DataObject):
    """
    @ Brief a message object used to set state and 
    """
    # Beware of using a class object as a typed attribute!
    lcstate = TypedAttribute(LCState, default=None)
    reference = TypedAttribute(ResourceReference)


class ResourceDescriptionListContainer(DataObject):
    """
    @ Brief a message object used to pass a list of resource description objects
    """
    resources = TypedAttribute(list, default=None)


class FindResourceDescriptionContainer(DataObject):
    """
    @ Brief a message object used to find resource description in a registry
    @ note string_comparison_method can be 'regex' or '=='
    """
    description = TypedAttribute(ResourceDescription, default=None)
    regex = TypedAttribute(bool, default=True)
    ignore_defaults = TypedAttribute(bool, default=True)