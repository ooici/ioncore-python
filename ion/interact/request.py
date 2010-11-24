#!/usr/bin/env python

"""
@file ion/interact/request.py
@author Michael Meisinger
@brief Request conversation type, as specified by FIPA spec SC00026H
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.data.dataobject import DataObject
from ion.interact.conversation import ConversationType, ConversationTypeSpec, Conversation, ConversationRole, ConversationTypeFSMFactory, RoleSpec
from ion.util.state_object import BasicStates


class RequestFSMFactory(ConversationTypeFSMFactory):
    """
    A FSM factory for FSMs with conversation type state model for request.
    This is for the initiator and participant roles.
    """

    S_INIT = BasicStates.S_INIT
    S_REQUESTED = "REQUESTED"
    S_REFUSED = "REFUSED"
    S_AGREED = "AGREED"
    S_FAILED = "FAILED"
    S_DONE = "DONE"
    S_ERROR = BasicStates.S_ERROR

    E_REQUEST = "request"
    E_REFUSE = "refuse"
    E_AGREE = "agree"
    E_FAILURE = "failure"
    E_DONE = "inform_done"
    E_DONE_RESULT = "inform_result"
    E_ERROR = "error"

    def create_fsm(self, target, memory=None):
        fsm = ConversationTypeFSMFactory.create_fsm(self, target, memory)

        actf = target._action

        actionfct = self._create_action_func(actf, self.E_REQUEST)
        fsm.add_transition(self.E_REQUEST, self.S_INIT, actionfct, self.S_REQUESTED)

        actionfct = self._create_action_func(actf, self.E_REFUSE)
        fsm.add_transition(self.E_REFUSE, self.S_REQUESTED, actionfct, self.S_REFUSED)

        actionfct = self._create_action_func(actf, self.E_AGREE)
        fsm.add_transition(self.E_AGREE, self.S_REQUESTED, actionfct, self.S_AGREED)

        actionfct = self._create_action_func(actf, self.E_FAILURE)
        fsm.add_transition(self.E_FAILURE, self.S_AGREED, actionfct, self.S_FAILED)

        # @todo Distinguish inform-done and inform-result input symbols?
        actionfct = self._create_action_func(actf, self.E_DONE)
        fsm.add_transition(self.E_DONE, self.S_AGREED, actionfct, self.S_DONE)
        fsm.add_transition(self.E_DONE_RESULT, self.S_AGREED, actionfct, self.S_DONE)

        actionfct = self._create_action_func(actf, self.E_ERROR)
        fsm.set_default_transition(actionfct, self.S_ERROR)

        return fsm

class Request(Conversation):
    """
    @brief Conversation instance for a request
    """

class RequestInitiator(ConversationRole):
    factory = RequestFSMFactory()

class RequestParticipant(ConversationRole):
    factory = RequestFSMFactory()

class RequestType(ConversationType):
    """
    @brief Defines the conversation type of request with its roles and id.
    """

    CONV_TYPE_REQUEST = "request"

    ROLE_INITIATOR = RoleSpec(
                        role_id="initiator",
                        role_class=RequestInitiator)
    ROLE_PARTICIPANT = RoleSpec(
                        role_id="participant",
                        role_class=RequestParticipant)

    roles = {ROLE_INITIATOR.role_id:ROLE_INITIATOR,
             ROLE_PARTICIPANT.role_id:ROLE_PARTICIPANT}

    def new_conversation(self, **kwargs):
        conv = Request(**kwargs)
        return conv
