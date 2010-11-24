#!/usr/bin/env python

"""
@file ion/interact/rpc.py
@author Michael Meisinger
@brief RPC conversation type
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.interact.conversation import ConversationType, Conversation, ConversationRole, ConversationTypeFSMFactory, RoleSpec
from ion.util.state_object import BasicStates


class RpcFSMFactory(ConversationTypeFSMFactory):
    """
    A FSM factory for FSMs with conversation type state model for rpc.
    RPC is like request without the accept/reject.
    This is for the initiator and participant roles.
    """

    S_INIT = BasicStates.S_INIT
    S_REQUESTED = "REQUESTED"
    S_FAILED = "FAILED"
    S_DONE = "DONE"
    S_ERROR = BasicStates.S_ERROR

    E_REQUEST = "request"
    E_FAILURE = "failure"
    E_DONE = "inform_done"
    E_DONE_RESULT = "inform_result"
    E_ERROR = "error"

    def create_fsm(self, target, memory=None):
        fsm = ConversationTypeFSMFactory.create_fsm(self, target, memory)

        actf = target._action

        actionfct = self._create_action_func(actf, self.E_REQUEST)
        fsm.add_transition(self.E_REQUEST, self.S_INIT, actionfct, self.S_REQUESTED)

        actionfct = self._create_action_func(actf, self.E_FAILURE)
        fsm.add_transition(self.E_FAILURE, self.S_REQUESTED, actionfct, self.S_FAILED)

        # @todo Distinguish inform-done and inform-result input symbols?
        actionfct = self._create_action_func(actf, self.E_DONE)
        fsm.add_transition(self.E_DONE, self.S_REQUESTED, actionfct, self.S_DONE)
        fsm.add_transition(self.E_DONE_RESULT, self.S_REQUESTED, actionfct, self.S_DONE)

        actionfct = self._create_action_func(actf, self.E_ERROR)
        fsm.set_default_transition(actionfct, self.S_ERROR)

        return fsm

class Rpc(Conversation):
    """
    @brief Conversation instance for a RPC
    """

class RpcInitiator(ConversationRole):
    factory = RpcFSMFactory()

class RpcParticipant(ConversationRole):
    factory = RpcFSMFactory()

class RpcType(ConversationType):
    """
    @brief Defines the conversation type of rpc with its roles and id.
    """

    CONV_TYPE_RPC = "rpc"

    ROLE_INITIATOR = RoleSpec(
                        role_id="initiator",
                        role_class=RpcInitiator)
    ROLE_PARTICIPANT = RoleSpec(
                        role_id="participant",
                        role_class=RpcParticipant)

    roles = {ROLE_INITIATOR.role_id:ROLE_INITIATOR,
             ROLE_PARTICIPANT.role_id:ROLE_PARTICIPANT}

    def new_conversation(self, **kwargs):
        conv = Rpc(**kwargs)
        return conv

class GenericType(ConversationType):
    """
    @brief Defines the conversation type of unspecified interactions.
    """

    CONV_TYPE_GENERIC = "generic"

    roles = {}

    def new_conversation(self, **kwargs):
        conv = Conversation(**kwargs)
        return conv
