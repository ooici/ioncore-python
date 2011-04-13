#!/usr/bin/env python

"""
@file ion/interact/request.py
@author Michael Meisinger
@brief Request conversation type, as specified by FIPA spec SC00026H
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
import ion.util.procutils as pu


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
    S_UNEXPECTED = "UNEXPECTED"    # When an event happens that is undefined
    S_TIMEOUT = "TIMEOUT"

    E_REQUEST = "request"
    E_REFUSE = "refuse"
    E_AGREE = "agree"
    E_FAILURE = "failure"
    E_RESULT = "inform_result"
    E_ERROR = "error"
    E_TIMEOUT = "timeout"

    A_UNEXPECTED = "unexpected"

    def create_fsm(self, target, memory=None):
        fsm = ConversationTypeFSMFactory.create_fsm(self, target, memory)

        actf = target._action

        # FSM definition
        # Notation STATE ^event --> NEW-STATE /callback-action-function

        # INIT ^request --> REQUESTED /request
        actionfct = self._create_action_func(actf, self.E_REQUEST)
        fsm.add_transition(self.E_REQUEST, self.S_INIT, actionfct, self.S_REQUESTED)

        # REQUESTED ^refuse --> REFUSED /refuse
        actionfct = self._create_action_func(actf, self.E_REFUSE)
        fsm.add_transition(self.E_REFUSE, self.S_REQUESTED, actionfct, self.S_REFUSED)

        # REQUESTED ^agree --> AGREED /agree
        actionfct = self._create_action_func(actf, self.E_AGREE)
        fsm.add_transition(self.E_AGREE, self.S_REQUESTED, actionfct, self.S_AGREED)

        # AGREED ^failure -->  FAILED /failure
        actionfct = self._create_action_func(actf, self.E_FAILURE)
        fsm.add_transition(self.E_FAILURE, self.S_AGREED, actionfct, self.S_FAILED)

        # AGREED ^inform_result -->  DONE /inform_result
        actionfct = self._create_action_func(actf, self.E_RESULT)
        fsm.add_transition(self.E_RESULT, self.S_AGREED, actionfct, self.S_DONE)

        # ANY ^error -->  ERROR /error
        actionfct = self._create_action_func(actf, self.E_ERROR)
        fsm.set_default_transition(actionfct, self.S_ERROR)

        return fsm

class Request(Conversation):
    """
    @brief Conversation instance for a request
    """

class RequestInitiator(ConversationRole):
    """
    Request Conversation Type. INITIATOR >>>> role.
    """
    factory = RequestFSMFactory()

    def request(self, message, *args, **kwargs):
        """
        @brief OUT msg. Send a request message
        """
        log.debug("OUT: Request.request")

    def refuse(self, message, *args, **kwargs):
        log.debug("In Request.refuse")

    def agree(self, message, *args, **kwargs):
        log.debug("In Request.agree")

    def failure(self, message, *args, **kwargs):
        """
        @brief IN msg. Receive a request message
        """
        log.debug("IN: Request.failure")
        return self.inform_result(message, *args, **kwargs)

    @defer.inlineCallbacks
    def inform_result(self, message, *args, **kwargs):
        conv = message['conversation']
        headers = message['headers']
        content = message['content']
        process = message['process']
        msg = message['msg']

        log.info('>>> [%s] Received Request inform_result <<<' % (process.proc_name))

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

class RequestParticipant(ConversationRole):
    """
    Request Conversation Type. >>>> PARTICIPANT role.
    """
    factory = RequestFSMFactory()

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

        log.info('>>> [%s] Received Request request for op=%s<<<' % (process.proc_name, headers['op']))

        # Send an agree if a request message
        yield process.reply_agree(msg)
        log.debug("[%s] send of agree message done" % (process.proc_name))

        # Note: We are NOW in a different conversation state: AGREED/REFUSED

        # Invoke operation/action
        try:
            res = yield process._dispatch_message_op(headers, msg, conv)
            defer.returnValue(res)
        except ApplicationError, ex:
            # In case of an application error - do not terminate the process!
            log.exception("*****Application error in message processing*****")
            # @todo Should we send an err or rather reject the msg?
            # @note We can only send a reply_err to an RPC
            if msg and msg.payload['reply-to'] and msg.payload.get('performative',None)=='request':
                yield process.reply_err(msg, exception = ex)
        except Exception, ex:
            # *** PROBLEM. Here the conversation is in ERROR state
            log.exception("*****Container error in message processing*****")
            # @todo Should we send an err or rather reject the msg?
            # @note We can only send a reply_err to an RPC
            if msg and msg.payload['reply-to'] and msg.payload.get('performative',None)=='request':
                yield process.reply_err(msg, exception = ex)

    def refuse(self, message, *args, **kwargs):
        log.debug("IN: Request.refuse")

    def agree(self, message, *args, **kwargs):
        log.debug("IN: Request.agree")

    def failure(self, message, *args, **kwargs):
        """
        @brief OUT msg. Reply with a failure in request processing
        """
        log.debug("OUT: Request.failure")

    def inform_result(self, message, *args, **kwargs):
        """
        @brief OUT msg. Reply with a failure in request processing
        """
        log.debug("OUT: Request.inform_result")

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

    DEFAULT_ROLE_INITIATOR = ROLE_INITIATOR.role_id
    DEFAULT_ROLE_PARTICIPANT = ROLE_PARTICIPANT.role_id

    FINAL_STATES = (RequestFSMFactory.S_DONE, RequestFSMFactory.S_FAILED,
                    RequestFSMFactory.S_ERROR, RequestFSMFactory.S_TIMEOUT, RequestFSMFactory.S_UNEXPECTED)

    def new_conversation(self, **kwargs):
        conv = Request(**kwargs)
        return conv
