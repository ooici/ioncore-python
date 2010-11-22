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
from ion.util.state_object import FSMFactory, StateObject, BasicStates

class ConversationRole(StateObject):
    """
    @brief A conversation as seen from one participant (=role binding).
        Encapsulates a FSM that keeps track of the state of the conversation
        of the participant.
    """
    def __init__(self):
        StateObject.__init__(self)
        factory = self.factory()
        fsm = factory.create_fsm(self)
        self._so_set_fsm(fsm)

class Conversation(DataObject):
    """
    @brief An instance of a conversation type. Identifies the entities by name
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
    """
    @brief Represents a conversation type. Also known as protocol, interaction
    pattern, session type.
    """

    def __init__(self, name=None, id=None, roles=None, spec=None, desc=None):
        """
        @brief Initializes the core attributes of a conversation type.
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
    """
    Represents a conversation type specification. Base class for specific
    specification languages, such as Scribble, MSC etc.
    """

class ConversationTypeFSMFactory(FSMFactory):
    """
    A factory for instantiating conversation type FSMs.
    If there are only two participants to a conversation, the same FSM can be
    used (with different action behavior) for the state of the participant
    conversations.
    """

    def _create_action_func(self, target, action):
        """
        @retval a function with a closure with the action name
        """
        def action_target(fsm):
            return target(action, fsm)
        return action_target

class ConversationManager(object):
    """
    @brief Oversees a set of conversations, e.g. within a process instance
    """

    # @todo CHANGE: Conversation ID counter
    convIdCnt = 0

    def __init__(self, process):
        self.process = process
        self.conversations = {}

    def msg_sent(self, message):
        pass

    def msg_received(self, message):
        pass

    def create_conversation_id(self):
        # Returns a new unique conversation id
        self.convIdCnt += 1
        convid = "#" + str(self.convIdCnt)
        #send = self.process.id.full
        #convid = send + "#" + Process.convIdCnt
        return convid
