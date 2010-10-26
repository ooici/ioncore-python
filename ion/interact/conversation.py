#!/usr/bin/env python

"""
@file ion/interact/conversation.py
@author Michael Meisinger
@brief classes for using conversations and conversation types (aka protocols,
    interaction patterns)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.data.dataobject import DataObject

class Conversation(DataObject):
    """An instance of a conversation type. Identifies the entities by name
    that bind to roles.  
    """
    def __init__(self, id=None, roleBindings=None, convType=None):
        """Initializes the core attributes of a conversation (instance.
        
        @param id    Unique registry identifier of a conversation
        @param roleBindings Mapping of names to role identifiers
        @param convType  Identifier for the conversation type
        """
        self.id = id
        self.roleBindings = roleBindings
        self.convType = convType
   
class ConversationType(DataObject):
    """Represents a conversation type. Also known as protocol, interaction
    pattern, session type. 
    """
    def __init__(self, name=None, id=None, roles=None, spec=None, desc=None):
        """Initializes the core attributes of a conversation type.
        
        @param name  Descriptive name of a conversation type
        @param id    Unique registry identifier of a conversation type
        @param roles List of interacting roles in an interaction pattern that
                processes can bind to
        @param spec  Protocol specification (e.g. Scribble)
        @param desc  Descriptive text
        """
        self.name = name
        self.id = id
        self.desc = desc
        self.roles = roles if roles else []
        self.spec = spec

class ConversationTypeSpec(DataObject):
    """Represents a conversation type specification. Base class for specific
    specification languages, such as Scribble, MSC etc.
    """
    
