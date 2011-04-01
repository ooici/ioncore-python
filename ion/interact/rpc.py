#!/usr/bin/env python

"""
@file ion/interact/rpc.py
@author Michael Meisinger
@brief RPC conversation type
"""

from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import failure

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.exception import ReceivedError, ApplicationError, ReceivedApplicationError, ReceivedContainerError
from ion.core.messaging.ion_reply_codes import ResponseCodes
from ion.core.messaging.message_client import MessageInstance
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
    E_RESULT = "inform_result"
    E_ERROR = "error"

    def create_fsm(self, target, memory=None):
        fsm = ConversationTypeFSMFactory.create_fsm(self, target, memory)

        actf = target._action

        # FSM definition
        # Notation STATE ^event --> NEW-STATE /callback-action-function

        # INIT ^request --> REQUESTED /request
        actionfct = self._create_action_func(actf, self.E_REQUEST)
        fsm.add_transition(self.E_REQUEST, self.S_INIT, actionfct, self.S_REQUESTED)

        # REQUESTED ^failure -->  FAILED /failure
        actionfct = self._create_action_func(actf, self.E_FAILURE)
        fsm.add_transition(self.E_FAILURE, self.S_REQUESTED, actionfct, self.S_FAILED)

        # REQUESTED ^inform_result -->  DONE /inform_result
        actionfct = self._create_action_func(actf, self.E_RESULT)
        fsm.add_transition(self.E_RESULT, self.S_REQUESTED, actionfct, self.S_DONE)

        # ANY ^error -->  ERROR /error
        actionfct = self._create_action_func(actf, self.E_ERROR)
        fsm.set_default_transition(actionfct, self.S_ERROR)

        return fsm

class Rpc(Conversation):
    """
    @brief Conversation instance for a RPC
    """

class RpcInitiator(ConversationRole):
    factory = RpcFSMFactory()

    def request(self, message, *args, **kwargs):
        """
        @brief OUT msg. Send a request message
        """
        log.debug("OUT: Rpc.request")

    def failure(self, message, *args, **kwargs):
        """
        @brief IN msg. Receive a request message
        """
        log.debug("IN: Rpc.failure")
        return self.inform_result(*args, **kweargs)

    @defer.inlineCallbacks
    def inform_result(self, message, *args, **kwargs):
        conv = message['conversation']
        headers = message['headers']
        content = message['content']
        process = message['process']
        msg = message['msg']

        log.info('>>> [%s] Received RPC inform_result <<<' % (process.proc_name))

        rpc_deferred = conv.blocking_deferred
        if conv.timeout:
            log.error("Message received after process %s RPC conv-id=%s timed out=%s: %s" % (
                process.proc_name, headers['conv-id'], rpc_deferred, headers))
            return
        if rpc_deferred:
            rpc_deferred.rpc_call.cancel()
        res = (content, headers, msg)

        yield msg.ack()

        status = headers.get(process.MSG_STATUS, None)
        if status == process.ION_OK:
            if rpc_deferred:
                #Cannot do the callback right away, because the message is not yet handled
                reactor.callLater(0, lambda: rpc_deferred.callback(res))
            else:
                log.error("ERROR. Do not support non-blocking RPC yet")

        elif status == process.ION_ERROR:
            code = -1
            if isinstance(content, MessageInstance):
                code = content.MessageResponseCode

            if 400 <= code and code < 500:
                err = failure.Failure(ReceivedApplicationError(headers, content))
            elif 500 <= code and code < 600:
                err = failure.Failure(ReceivedContainerError(headers, content))
            else:
                # Received Error is still the catch all!
                err = failure.Failure(ReceivedError(headers, content))

            if rpc_deferred:
                # Cannot do the callback right away, because the message is not yet handled
                reactor.callLater(0, lambda: rpc_deferred.errback(err))
            else:
                log.error("ERROR. Do not support non-blocking RPC yet")

        else:
            log.error('RPC reply is not well formed. Header "status" must be set!')
            if rpc_deferred:
                #Cannot do the callback right away, because the message is not yet handled
                reactor.callLater(0, lambda: rpc_deferred.callback(res))
            else:
                log.error("ERROR. Do not support non-blocking RPC yet")


        log.debug('[%s] RPC inform_result done.' % (process.proc_name))

class RpcParticipant(ConversationRole):
    factory = RpcFSMFactory()

    @defer.inlineCallbacks
    def request(self, message, *args, **kwargs):
        """
        @brief IN msg. Receive a request message
        """
        conv = message['conversation']
        headers = message['headers']
        content = message['content']
        process = message['process']
        msg = message['msg']

        log.info('>>> [%s] Received RPC request for op=%s<<<' % (process.proc_name, headers['op']))

        # Send an agree if a request message
        conv_type = headers.get('protocol', GenericType.CONV_TYPE_GENERIC)

        # Check for operation/action
        res = yield process._dispatch_message_op(headers, msg, conv)
        defer.returnValue(res)

    def failure(self, message, *args, **kwargs):
        """
        @brief OUT msg. Reply with a failure in request processing
        """
        log.debug("OUT: Rpc.failure")

    def inform_result(self, message, *args, **kwargs):
        """
        @brief OUT msg. Reply with a failure in request processing
        """
        log.debug("OUT: Rpc.inform_result")

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

    DEFAULT_ROLE_INITIATOR = ROLE_INITIATOR.role_id
    DEFAULT_ROLE_PARTICIPANT = ROLE_PARTICIPANT.role_id

    def new_conversation(self, **kwargs):
        conv = Rpc(**kwargs)
        return conv

class GenericFSMFactory(ConversationTypeFSMFactory):
    """
    A dummy FSM factory .
    """

    S_INIT = BasicStates.S_INIT
    E_REQUEST = "request"

    def create_fsm(self, target, memory=None):
        fsm = ConversationTypeFSMFactory.create_fsm(self, target, memory)

        actf = target._action
        actionfct = self._create_action_func(actf, self.E_REQUEST)
        fsm.add_transition(self.E_REQUEST, self.S_INIT, actionfct, self.S_INIT)
        fsm.set_default_transition(actionfct, self.S_INIT)
        return fsm

class GenericInitiator(ConversationRole):
    factory = GenericFSMFactory()

    def request(self, message, *args, **kwargs):
        log.debug("In Generic.request (IN)")

class GenericParticipant(ConversationRole):
    factory = GenericFSMFactory()

    def request(self, message, *args, **kwargs):
        log.debug("In Generic.request (OUT)")
        # Check for operation/action
        return message['process']._dispatch_message_op(message['headers'],
                                            message['msg'], message['conversation'])

class GenericType(ConversationType):
    """
    @brief Defines the conversation type of unspecified interactions.
    """

    CONV_TYPE_GENERIC = "generic"

    ROLE_INITIATOR = RoleSpec(
                        role_id="initiator",
                        role_class=GenericInitiator)
    ROLE_PARTICIPANT = RoleSpec(
                        role_id="participant",
                        role_class=GenericParticipant)

    roles = {ROLE_INITIATOR.role_id:ROLE_INITIATOR,
             ROLE_PARTICIPANT.role_id:ROLE_PARTICIPANT}

    DEFAULT_ROLE_INITIATOR = ROLE_INITIATOR.role_id
    DEFAULT_ROLE_PARTICIPANT = ROLE_PARTICIPANT.role_id

    def new_conversation(self, **kwargs):
        conv = Conversation(**kwargs)
        return conv
