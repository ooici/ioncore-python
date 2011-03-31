#!/usr/bin/env python

"""
@file ion/core/process/process.py
@author Michael Meisinger
@brief base classes for processes within a capability container
"""

import traceback
from twisted.internet import defer
from twisted.internet import reactor
from twisted.python import failure

from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ReceivedError, ApplicationError, ReceivedApplicationError, ReceivedContainerError
from ion.core.id import Id
from ion.core.intercept.interceptor import Interceptor
from ion.core.messaging.receiver import ProcessReceiver
from ion.core.messaging.ion_reply_codes import ResponseCodes
from ion.core.messaging.message_client import MessageClient, MessageInstance

from ion.core.process.cprocess import IContainerProcess, ContainerProcess
from ion.data.store import Store
from ion.interact.conversation import ProcessConversationManager, CONV_TYPE_NONE
from ion.interact.message import Message
from ion.interact.request import RequestType
from ion.interact.rpc import RpcType, GenericType
from ion.util.context import StackLocal
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject, BasicStates

from ion.core.object import workbench

CONF = ioninit.config(__name__)
CF_fail_fast = CONF['fail_fast']
CF_rpc_timeout = CONF['rpc_timeout']

# @todo CHANGE: Dict of "name" to process (service) declaration
processes = {}

# @todo CHANGE: Static store (kvs) to register process instances with names
procRegistry = Store()

# Static entry point for "thread local" context storage during request
# processing, eg. to retaining user-id from request message
request = StackLocal()


class IProcess(Interface):
    """
    Interface for all capability container application processes
    """

class ProcessError(Exception):
    """
    An exception class for errors that occur in Process
    """

class Process(BasicLifecycleObject, ResponseCodes):
    """
    This is the base class for all processes. Processes can be spawned and
    have a unique identifier. Each process has one main process receiver and can
    define additional receivers as needed. This base class provides a lot of
    mechanics for processes, such as sending and receiving messages, RPC style
    calls, spawning and terminating child processes. Subclasses may use the
    plc-* process life cycle events.
    """
    implements(IProcess)

    def __init__(self, receiver=None, spawnargs=None, **kwargs):
        """
        Initialize process using an optional receiver and optional spawn args
        @param receiver instance of a Receiver for process control (unused)
        @param spawnargs standard and additional spawn arguments
        """
        BasicLifecycleObject.__init__(self)

        spawnargs = spawnargs.copy() if spawnargs else {}
        self.spawn_args = spawnargs
        self.proc_init_time = pu.currenttime_ms()

        # An Id with the process ID (fully qualified)
        procid = self.spawn_args.get('proc-id', ProcessInstantiator.create_process_id())
        procid = pu.get_process_id(procid)
        self.id = procid
        assert isinstance(self.id, Id), "Process id must be Id"

        # Name (human readable label) of this process.
        self.proc_name = self.spawn_args.get('proc-name', __name__)

        # The system unique name; propagates from root supv to all child procs
        default_sysname = ioninit.sys_name or Id.default_container_id
        self.sys_name = self.spawn_args.get('sys-name', default_sysname)

        # An Id with the process ID of the parent (supervisor) process
        self.proc_supid = pu.get_process_id(self.spawn_args.get('sup-id', None))

        # Name (human readable label) of this process.
        self.proc_group = self.spawn_args.get('proc-group', self.proc_name)

        # Set the container
        self.container = ioninit.container_instance

        # Ignore supplied receiver for consistency purposes
        # Create main receiver; used for incoming process interactions
        self.receiver = ProcessReceiver(
                                    label=self.proc_name,
                                    name=self.id.full,
                                    group=self.proc_group,
                                    process=self,
                                    handler=self.receive,
                                    error_handler=self.receive_error)

        # Create a backend receiver for outgoing RPC process interactions.
        # Needed to avoid deadlock when processing incoming messages
        # because only one message can be consumed before ACK.
        self.backend_id = Id(self.id.local+"b", self.id.container)
        self.backend_receiver = ProcessReceiver(
                                    label=self.proc_name,
                                    name=self.backend_id.full,
                                    group=self.proc_group,
                                    process=self,
                                    handler=self.receive,
                                    error_handler=self.receive_error)

        # Dict of all receivers of this process. Key is the name
        self.receivers = {}
        self.add_receiver(self.receiver)
        self.add_receiver(self.backend_receiver)

        # Delegate class to manage all conversations of this process
        self.conv_manager = ProcessConversationManager(self)

        # Dict of message publishers and subscribers
        self.publishers = {}
        self.subscribers = {}

        # List of ProcessDesc instances of defined and spawned child processes
        self.child_procs = []

        #The data object Workbench for all object repositories used by this process
        self.workbench = workbench.WorkBench(self)

        # Create a message Client
        self.message_client = MessageClient(proc=self)

        # A list of other kinds of life cycle objects which are tied to the process
        self._registered_life_cycle_objects = []

        # TCP Connectors and Listening Ports
        self.connectors = []
        self.listeners = []

        log.debug("NEW Process instance [%s]: id=%s, sup-id=%s, sys-name=%s" % (
                self.proc_name, self.id, self.proc_supid, self.sys_name))

    # --- Life cycle management
    # Categories:
    # op_XXX Message incoming interface
    # spawn, init: Boilerplate API
    # initialize, activate, deactivate, terminate: (Super class) State management API
    # on_XXX: State management API action callbacks
    # plc_XXX: Callback hooks for subclass processes

    @defer.inlineCallbacks
    def spawn(self):
        """
        Manually (instead of through the container) spawns this process and
        activate it in the same call. Spawn can only be called once.
        Equivalent to calling initialize() and activate()
        @retval Deferred for the Id of the process (self.id)
        """
        yield self.initialize()
        yield self.activate()
        yield ioninit.container_instance.proc_manager.register_local_process(self)
        defer.returnValue(self.id)

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        LifeCycleObject callback for the initialization "spawn" of the process.
        @retval Deferred for the Id of the process (self.id)
        """
        assert not self.backend_receiver.consumer, "Process already initialized"
        log.debug('Process [%s] id=%s initialize()' % (self.proc_name, self.id))

        # Create queue only for process receiver
        yield self.receiver.initialize()

        # Create queue and consumer for backend receiver
        yield self.backend_receiver.initialize()
        yield self.backend_receiver.activate()

        # Move registerd life cycle objects through there life cycle
        for lco in self._registered_life_cycle_objects:
            yield defer.maybeDeferred(lco.initialize)

        # Callback to subclasses
        try:
            yield defer.maybeDeferred(self.plc_init)
            log.info('Process [%s] id=%s: INIT OK' % (self.proc_name, self.id))
        except Exception, ex:
            log.exception('----- Process %s INIT ERROR -----' % (self.id))
            raise ex

    def plc_init(self):
        """
        Process life cycle event: on initialization of process (once)
        """

    @defer.inlineCallbacks
    def op_activate(self, content, headers, msg):
        """
        Activate operation, on receive of the activate system message
        @note PROBLEM: Cannot receive activate if receiver not active.
                Activation has to go through the container (agent) or backend.
        """
        try:
            yield self.activate(content, headers, msg)
            if msg != None:
                yield self.reply_ok(msg)
        except Exception, ex:
            if msg != None:
                yield self.reply_uncaught_err(msg, content=None, exception=ex,
                                              response_code="Process %s ACTIVATE ERROR" % (self.id))

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        LifeCycleObject callback for activate
        @retval Deferred
        """
        log.debug('Process [%s] id=%s activate()' % (self.proc_name, self.id))

        # Create consumer for process receiver
        yield self.receiver.activate()

        # Move registerd life cycle objects through there life cycle
        for lco in self._registered_life_cycle_objects:
            yield defer.maybeDeferred(lco.activate)

        # Callback to subclasses
        try:
            yield defer.maybeDeferred(self.plc_activate)
        except Exception, ex:
            log.exception('----- Process %s ACTIVATE ERROR -----' % (self.id))
            raise ex

    def plc_activate(self):
        """
        Process life cycle event: on activate of process. Subclasses override.
        """

    def shutdown(self):
        return self.terminate()

    @defer.inlineCallbacks
    def op_terminate(self, content, headers, msg):
        """
        Shutdown operation, on receive of the init message
        """
        try:
            yield self.terminate(msg=msg)
            if msg != None:
                yield self.reply_ok(msg)
        except Exception, ex:

            log.error('Error during op_terminate: ' + str(ex))
            raise ProcessError("Process %s TERMINATE ERROR" % (self.id))
            ### Let the mesg dispatcher catch the error
            #if msg != None:
            #    yield self.reply_err(msg, content=None, exception=ex, response_code = "Process %s TERMINATE ERROR" % (self.id))

    @defer.inlineCallbacks
    def on_terminate_active(self, *args, **kwargs):
        # This is temporary while there is no deactivate for a process (don't
        # want to do this right now).
        yield self.receiver.deactivate()

        term_msg = kwargs.get('msg', None)
        if term_msg:
            yield self.receiver._await_message_processing(term_msg_id=id(term_msg))
        else:
            yield self.receiver._await_message_processing()

        yield self.on_terminate(*args, **kwargs)

        yield self.backend_receiver.deactivate()
        yield self.backend_receiver._await_message_processing()

    @defer.inlineCallbacks
    def on_terminate(self, msg=None, *args, **kwargs):
        """
        @retval Deferred
        """
        # Clean up all TCP connections and listening ports
        for connector in self.connectors:
            # XXX What is the best way to unit test this?
            connector.disconnect()
        for port in self.listeners:
            yield port.stopListening()

        # Move registerd life cycle objects through there life cycle
        for lco in self._registered_life_cycle_objects:
            yield defer.maybeDeferred(lco.terminate)

        if len(self.child_procs) > 0:
            log.info("Shutting down child processes")
        while len(self.child_procs) > 0:
            child = self.child_procs.pop()
            try:
                res = yield self.shutdown_child(child)
            except Exception, ex:
                log.exception("Error terminating child %s" % child.proc_id)

        yield defer.maybeDeferred(self.plc_terminate)
        log.info('----- Process %s TERMINATED -----' % (self.proc_name))

    def plc_terminate(self):
        """
        Process life cycle event: on termination of process (once)
        """

    def on_error(self, cause= None, *args, **kwargs):
        if cause:
            log.error("Process error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal process state change")

    def is_spawned(self):
        return self.receiver.consumer != None

    # --- Non-lifecycle process interactions

    @defer.inlineCallbacks
    def op_ping(self, content, headers, msg):
        """
        Service operation: ping reply
        """
        yield self.reply_ok(msg, {'pong':'pong'}, {'quiet':True})

    #    @defer.inlineCallbacks
    def op_sys_procexit(self, content, headers, msg):
        """
        Called when a child process has exited without being terminated. A
        supervisor may process this event and restart the child.
        """
        pass

    # --- Process resource management

    def add_receiver(self, receiver):
        self.receivers[receiver.name] = receiver

    def add_publisher(self, publisher):
        """
        Adds a publisher to this process.

        @param  publisher   A Publisher instance, preferably tied to this process.
        """
        self.publishers[publisher.name] = publisher

    def add_subscriber(self, subscriber):
        """
        Adds a subscriber to this process, preferably tied to this process.
        """
        self.subscribers[subscriber.name] = subscriber

    def connectTCP(self, host, port, factory, timeout=30, bindAddress=None):
        connector = reactor.connectTCP(host, port, factory, timeout, bindAddress)
        self.connectors.append(connector)
        return connector

    def listenTCP(self, port, factory, backlog=50, interface=''):
        port = reactor.listenTCP(port, factory, backlog, interface)
        self.listeners.append(port)
        return port

    def add_life_cycle_object(self, lco):
        """
        Add a life cycle object to the process during init.
        This method can only be used during init to add lco's which are also
        in the INIT state.
        """
        assert isinstance(lco, BasicLifecycleObject), \
            'Can not add an instance that does not inherit from BasicLifecycleObject'

        if self._get_state() != BasicStates.S_INIT:
            raise ProcessError, 'Can not use add_life_cycle_objects when the process is past the INIT state.'

        self._registered_life_cycle_objects.append(lco)

    @defer.inlineCallbacks
    def register_life_cycle_object(self, lco):
        """
        Register a life cycle object with the process and automatically advance
        its state to the current state of the process.
        """
        assert isinstance(lco, BasicLifecycleObject), \
        'Can not register an instance that does not inherit from BasicLifecycleObject'

        if self._get_state() == BasicStates.S_INIT:
            pass
        elif self._get_state() == BasicStates.S_READY:
            yield defer.maybeDeferred(lco.initialize)

        elif self._get_state() == BasicStates.S_ACTIVE:
            yield defer.maybeDeferred(lco.initialize)
            yield defer.maybeDeferred(lco.activate)

        elif self._get_state() == BasicStates.S_TERMINATED:
            raise ProcessError, '''Can not register life cycle object in invalid state "TERMINATED"'''
        elif self._get_state() == BasicStates.S_ERROR:
            raise ProcessError, '''Can not register life cycle object in invalid state "ERROR"'''

        self._registered_life_cycle_objects.append(lco)

    ##########################################################################
    # --- Incoming message handling

    @defer.inlineCallbacks
    def receive(self, payload, msg):
        """
        This is the first and MAIN entry point for received messages. Using the
        conv-id header, an ongoing conversation is retrieved and checked for a
        blocking condition (eg. RPC reply). Messages are separated whether
        blocking or not.
        """
        try:
            # Establish security context for request processing
            self._establish_request_context(payload, msg)

            # Extract some headers and make log statement.
            fromname = payload['sender']
            if 'sender-name' in payload:
                fromname = payload['sender-name']   # Legible sender alias
            log.info('>>> [%s] receive(): Message from [%s] ... >>>' % (
                     self.proc_name, fromname))
            convid = payload.get('conv-id', None)
            protocol = payload.get('protocol', None)

            # Conversation handling.
            conv = None
            if convid and protocol != CONV_TYPE_NONE:
                # Compose in memory message object for callbacks
                message = dict(recipient=payload.get('receiver',None),
                               performative=payload.get('performative','request'),
                               operation=payload.get('op',None),
                               headers=payload,
                               content=payload.get('content',None),
                               process=self,
                               msg=msg,
                               conversation=None)

                # Retrieve ongoing Conversation instance by conv-id header.
                # If not existing, create new instance.
                # @todo How do we find out we are the participant or initiator role?
                initiator = False
                conv = self.conv_manager.get_or_create_conversation(convid,
                                                message, initiator=initiator)
                message['conversation'] = conv

                # Detect and handle request to terminate process.
                # This does not yet work properly with fail fast...
                if not self._get_state() == "ACTIVE":
                    if 'op' in payload and payload['op'] == 'terminate':

                        if self._get_state() != BasicStates.S_TERMINATED:
                            yield self.terminate()
                        else:
                            yield self.reply_ok(msg)

                    text = "Process %s in invalid state %s." % (self.proc_name, self._get_state())
                    log.error(text)

                    # @todo: Requeue would be ok, but does not work (Rabbit or client limitation)
                    #d = msg.requeue()

                    # Let the error back handle the exception
                    raise ProcessError(text)

                # Regular message handling in expected state
                pu.log_message(msg)

                # Delegate further message processing to conversation specific
                # implementation. Trigger conversation FSM.
                # @see ion.interact.rpc, ion.interact.request
                res = yield self.conv_manager.msg_received(message)

            elif convid and protocol == CONV_TYPE_NONE:
                # Case of one-off messages
                log.debug("Received simple protocol=='none' message")
                pu.log_message(msg)
                res = yield self._dispatch_message_op(payload, msg, None)

            else:
                # Legacy case of no conv-id set or one-off messages (events?)
                log.warn("No conversation id in message")
                pu.log_message(msg)
                res = yield self._dispatch_message_op(payload, msg, None)

        except ApplicationError, ex:
            # In case of an application error - do not terminate the process!
            log.exception("*****Application error in message processing*****")
            # @todo Should we send an err or rather reject the msg?
            # @note We can only send a reply_err to an RPC
            if msg and msg.payload['reply-to'] and msg.payload.get('performative',None)=='request':
                yield self.reply_err(msg, exception = ex)

        except Exception, ex:
            log.exception("*****Container error in message processing*****")
            # @todo Should we send an err or rather reject the msg?
            # @note We can only send a reply_err to an RPC
            if msg and msg.payload['reply-to'] and msg.payload.get('performative',None)=='request':
                yield self.reply_err(msg, exception = ex)

            #@Todo How do we know if the message was ack'ed here?
            # The supervisor will also call shutdown child procs. This causes a recursive error when using fail fast!
            #if CF_fail_fast:
            #    yield self.terminate()
                # Send exit message to supervisor
        finally:
            # @todo This is late here (potentially after a reply_err before)
            if msg._state == "RECEIVED":
                # Only if msg has not been ack/reject/requeued before
                log.debug("<<< ACK msg")
                yield msg.ack()

    def _establish_request_context(self, payload, msg):
        """
        @brief Establish security context for request processing.
            Extract and set user-id, session expiry based on incoming headers
        """
        request.proc_name = self.proc_name
        # Check if there is a user id in the header, stash if so
        if 'user-id' in payload:
            log.info('[%s] receive(): payload user id [%s]' % (self.proc_name, payload['user-id']))
            request.user_id = payload.get('user-id')
            log.info('[%s] receive(): Set/updated stashed user_id: [%s]' % (self.proc_name, request.get('user_id')))
        else:
            log.info('[%s] receive(): payload anonymous request' % (self.proc_name))
            if request.get('user_id', 'Not set') == 'Not set':
                request.user_id = 'ANONYMOUS'
                log.info('[%s] receive(): Set stashed user_id to ANONYMOUS' % (self.proc_name))
            else:
                log.info('[%s] receive(): Kept stashed user_id the same: [%s]' % (self.proc_name, request.get('user_id')))
        # User session expiry.
        if 'expiry' in payload:
            request.expiry = payload.get('expiry')
            log.info('[%s] receive(): Set/updated stashed expiry: [%s]' % (self.proc_name, request.get('expiry')))
        else:
            if request.get('expiry', 'Not set') == 'Not set':
                request.expiry = '0'
                log.info('[%s] receive(): Set stashed expiry to 0' % (self.proc_name))
            else:
                log.info('[%s] receive(): Kept stashed expiry the same: [%s]' % (self.proc_name, request.get('expiry')))

    def _dispatch_message_op(self, payload, msg, conv):
        if "op" in payload:
            op = payload['op']
            opname = 'op_' + str(op)
            return self._dispatch_message_call(payload, msg, conv, opname)
        else:
            log.error("Invalid message. No 'op' in header", payload)

    @defer.inlineCallbacks
    def _dispatch_message_call(self, payload, msg, conv, opname):
        """
        Dispatch of messages to handler callback functions within this
        Process instance. If handler is not present, use op_none.
        @retval Deferred
        """
        content = payload.get('content','')
        # dynamically invoke the operation in the given class
        if hasattr(self, opname):
            opf = getattr(self, opname)
            yield defer.maybeDeferred(opf, content, payload, msg)
        elif hasattr(self,'op_none'):
            yield defer.maybeDeferred(self.op_none, content, payload, msg)
        else:
            assert False, "Cannot dispatch to operation"

    def op_none(self, content, headers, msg):
        """
        The method called if operation callback handler is not existing
        """
        log.error('Process does not define op=%s' % headers.get('op',None))

    # --- Standard conversation type support: RPC, Request

    def rpc_send(self, recv, operation, content, headers=None, **kwargs):
        """
        @brief Starts a simple RPC style conversation.
        """
        rpc_conv = self.conv_manager.new_conversation(RpcType.CONV_TYPE_RPC)
        rpc_conv.bind_role_local(RpcType.ROLE_INITIATOR.role_id, self)
        rpc_conv.bind_role(RpcType.ROLE_PARTICIPANT.role_id, recv)

        if headers == None:
            headers = {}
        headers['protocol'] = RpcType.CONV_TYPE_RPC
        headers['performative'] = 'request'
        return self._blocking_send(recv=recv, operation=operation,
                                   content=content, headers=headers,
                                   conv=rpc_conv, **kwargs)

    def request(self, receiver, action, content, headers=None, **kwargs):
        """
        @brief Sends a request message to the recipient. Instantiates the
            FIPA request interaction pattern. The recipient needs to responde
            with either "refuse" or "agree". In case of "agree", a second
            "failure", "inform-done", "inform-result" message must follow. This
            function checks for timeouts.
            Synchronous call.
        @exception Various exceptions for different failures
        """
        req_conv = self.conv_manager.new_conversation(RequestType.CONV_TYPE_REQUEST)
        req_conv.bind_role_local(RequestType.ROLE_INITIATOR.role_id, self)
        req_conv.bind_role(RequestType.ROLE_PARTICIPANT.role_id, receiver)

        if headers == None:
            headers = {}
        headers['protocol'] = RequestType.CONV_TYPE_REQUEST
        headers['performative'] = 'request'
        return self._blocking_send(recv=receiver, operation=action,
                                   content=content, headers=headers,
                                   conv=req_conv, **kwargs)

    def request_async(self, message, **kwargs):
        """
        @brief Sends a request message to the recipient. Asynchronous call.
        """
        pass

    # --- Outgoing message handling

    def _blocking_send(self, recv, operation, content, headers=None, conv=None, **kwargs):
        """
        @brief Sends a message and waits for conversation message reply.
        @retval a Deferred with the message value on receipt
        """
        #if headers:
        #    if 'user-id' in headers:
        #        log.info('>>> [%s] rpc_send(): headers user id [%s] <<<' % (self.proc_name, headers['user-id']))
        #    else:
        #        log.info('>>> [%s] rpc_send(): user-id not specified in headers <<<' % (self.proc_name))
        #else:
        #    log.info('>>> [%s] rpc_send(): headers not specified <<<' % (self.proc_name))
        msgheaders = {}
        if headers:
            msgheaders.update(headers)
        assert conv, "Conversation instance must exist for blocking send"
        msgheaders['conv-id'] = conv.conv_id

        # Create a new deferred that the caller can yield on to wait for RPC
        conv.blocking_deferred = defer.Deferred()
        # Timeout handling
        timeout = float(kwargs.get('timeout', CF_rpc_timeout))
        def _timeoutf():
            log.warn("Process %s RPC conv-id=%s timed out! " % (self.proc_name,conv.conv_id))
            # Remove RPC. Delayed result will go to catch operation
            conv.timeout = str(pu.currenttime_ms())
            conv.blocking_deferred.errback(defer.TimeoutError())
        if timeout:
            callto = reactor.callLater(timeout, _timeoutf)
            conv.blocking_deferred.rpc_call = callto

        # Call to send()
        d = self.send(recv, operation, content, msgheaders)
        # d is a deferred. The actual send of the request message will happen
        # after this method returns. This is OK, because functions are chained
        # to call back the caller on the rpc_deferred when the receipt is done.
        return conv.blocking_deferred

    def send(self, recv, operation, content, headers=None, reply=False):
        """
        @brief Send a message via the process receiver to destination.
            Starts a new conversation.
        @retval Deferred for send of message
        """
        msgheaders = {}
        msgheaders['sender-name'] = self.proc_name
        if headers:
            msgheaders.update(headers)

        #log.debug("****SEND, headers %s" % str(msgheaders))
        if 'conv-id' in msgheaders:
            conv = self.conv_manager.get_conversation(msgheaders['conv-id'])
            #log.debug("Send conversation %r from %r" % (conv, self.conv_manager.conversations))
        else:
            # Not a new and not a reply to a conversation
            #conv = self.conv_manager.new_conversation(GenericType.CONV_TYPE_GENERIC)
            #msgheaders['conv-id'] = conv.conv_id
            # One-off send message. Do not create a conversation instance.
            conv = None
            msgheaders['conv-id'] = self.conv_manager.create_conversation_id()
            msgheaders['protocol'] = CONV_TYPE_NONE

        if not 'user-id' in msgheaders:
            msgheaders['user-id'] = request.get('user_id', 'ANONYMOUS')
            log.debug('[%s] send(): set user id in msgheaders from stashed user_id [%s]' % (self.proc_name, msgheaders['user-id']))
        else:
            log.debug('[%s] send(): using user id from msgheaders [%s]' % (self.proc_name, msgheaders['user-id']))
        if not 'expiry' in msgheaders:
            msgheaders['expiry'] = request.get('expiry', '0')
            log.debug('[%s] send(): set expiry in msgheaders from stashed expiry [%s]' % (self.proc_name, msgheaders['expiry']))
        else:
            log.debug('[%s] send(): using expiry from msgheaders [%s]' % (self.proc_name, msgheaders['expiry']))

        message = dict(recipient=recv, operation=operation,
                       content=content, headers=msgheaders,
                       performative=msgheaders.get('performative','request'),
                       process=self, conversation=conv)

        # Put the message through the conversation FSM
        # @todo Must support deferred
        self.conv_manager.msg_send(message)

        if reply:
            d = self.receiver.send(**message)
        else:
            d = self.backend_receiver.send(**message)
        return d

    @defer.inlineCallbacks
    def receive_error(self, payload, msg, response_code=None):
        """
        This is the entry point for handling messaging errors. As appropriate,
        this method will attempt to respond with a meaningful error code to
        the sender.
        """
        if msg and msg.payload['reply-to']:
            yield self.reply_err(msg=msg, response_code=response_code)
        yield msg.ack()

    def reply(self, msg, operation=None, content=None, headers=None):
        """
        @brief Replies to a given message, continuing the ongoing conversation
        @retval Deferred or None
        """
        if operation is None:
            operation = self.MSG_RESULT

        ionMsg = msg.payload
        recv = ionMsg.get('reply-to', None)
        if headers is None:
            headers = {}

        if recv == None:
            log.error('No reply-to given for message '+str(msg))
        else:
            headers['conv-id'] = ionMsg.get('conv-id','')
            headers['conv-seq'] = int(ionMsg.get('conv-seq',0)) + 1
        if not 'user-id' in headers:
            headers['user-id'] = request.get('user_id', 'ANONYMOUS')
            log.debug('[%s] reply(): set user id [%s]' % (self.proc_name, headers['user-id']))
        if not 'expiry' in headers:
            headers['expiry'] = request.get('expiry', '0')
            log.debug('[%s] reply(): set expiry [%s]' % (self.proc_name, headers['expiry']))

        return self.send(pu.get_process_id(recv), operation, content, headers, reply=True)

    def reply_agree(self, msg, content=None, headers=None):
        """
        @brief Boilerplate method that replies to a given message with an agree
        @retval Deferred for send of reply
        """
        msgheaders = {}
        if headers != None:
            msgheaders.update(headers)
        msgheaders['performative'] = 'agree'
        msgheaders['protocol'] = msg.payload['protocol']
        # Note: Header status=OK is automatically set

        if content is None:
            content = yield self.message_client.create_instance(MessageContentTypeID=None)

        if isinstance(content, MessageInstance):
            if not content.Message.IsFieldSet('response_code'):
                content.MessageResponseCode = content.ResponseCodes.OK

        self.reply(msg, operation=self.MSG_RESULT, content=content, headers=msgheaders)

    @defer.inlineCallbacks
    def reply_ok(self, msg, content=None, headers=None):
        """
        @brief Boilerplate method that replies to a given message with a success
            message and a given result value
        @content any sendable type to be converted to dict, or dict (untouched)
        @retval Deferred for send of reply
        """
        msgheaders = {}
        if headers is not None:
            msgheaders.update(headers)
        msgheaders['performative'] = 'inform_result'
        msgheaders['protocol'] = msg.payload['protocol']
        # Note: Header status=OK is automatically set

        if content is None:
            content = yield self.message_client.create_instance(MessageContentTypeID=None)

        if isinstance(content, MessageInstance):
            if not content.Message.IsFieldSet('response_code'):
                content.MessageResponseCode = content.ResponseCodes.OK

        self.reply(msg, operation=self.MSG_RESULT, content=content, headers=msgheaders)


    @defer.inlineCallbacks
    def reply_err(self, msg, content=None, headers=None, exception=None, response_code=None):
        """
        @brief Boilerplate method for reply to a message which lead to an
            application level error. The result can include content, a caught
            exception and an application level error_code as an indication of the error.
        @content any sendable type to be converted to dict, or dict (untouched)
        @exception an instance of Exception
        @response_code an ION application level defined error code for a handled exception
        @retval Deferred for send of reply
        """
        if content is None:
            content = yield self.message_client.create_instance(MessageContentTypeID=None)

        if isinstance(content, MessageInstance):

            if not content.Message.IsFieldSet('response_code'):

                if isinstance(exception, ApplicationError):
                    content.MessageResponseCode = exception.response_code
                elif response_code:
                    content.MessageResponseCode = response_code
                else:
                    content.MessageResponseCode = content.ResponseCodes.INTERNAL_SERVER_ERROR

            if not content.Message.IsFieldSet('response_body'):
                content.MessageResponseBody = str(exception)

        reshdrs = {}
        #reshdrs['performative'] = 'failure'
        reshdrs['performative'] = 'inform_result'
        reshdrs['protocol'] = msg.payload['protocol']
        reshdrs[self.MSG_STATUS] = self.ION_ERROR

        if headers != None:
            reshdrs.update(headers)

        self.reply(msg, operation=self.MSG_RESULT, content=content, headers=reshdrs)

    def get_conversation(self, headers):
        convid = headers.get('conv-id', None)
        return self.conv_manager.get_conversation(convid)

    # --- Process and child process management

    def get_scoped_name(self, scope, name):

        # Proposed modificaiton to get_scoped_name to only add the scope if needed?
        prefix = pu.get_scoped_name('', scope)
        if prefix in name:
            return name
        else:
            return pu.get_scoped_name(name, scope)

    # OTP style functions for working with processes and modules/apps

    @defer.inlineCallbacks
    def spawn_child(self, childproc, activate=True):
        """
        Spawns a process described by the ProcessDesc instance as child of this
        process instance. An init message is sent depending on flag.
        @param childproc  ProcessDesc instance with attributes
        @param init  flag determining whether an init message should be sent
        @retval process id of the child process
        """
        assert isinstance(childproc, ProcessDesc)
        assert not childproc in self.child_procs, "Process already spawned"
        self.child_procs.append(childproc)
        child_id = yield childproc.spawn(self, activate=activate)
        yield procRegistry.put(str(childproc.proc_name), str(child_id))
        defer.returnValue(child_id)

    def link_child(self, supervisor):
        pass

    def spawn_link(self, childproc, supervisor):
        pass

    def shutdown_child(self, childproc):
        return childproc.shutdown()

    def get_child_def(self, name):
        """
        @retval the ProcessDesc instance of a child process by name
        """
        for child in self.child_procs:
            if child.proc_name == name:
                return child

    def get_child_id(self, name):
        """
        @retval the process id a child process by name
        """
        child = self.get_child_def(name)
        return child.proc_id if child else None

    def __str__(self):
        return "Process(id=%s,name=%s)" % (self.id, self.proc_name)

class AppInterceptor(Interceptor):
    def process(self, invocation):
        assert invocation.path == Invocation.PATH_IN
        defer.maybeDeferred(self.before, invocation)
        return invocation

# ============================================================================


class ProcessClientBase(object,ResponseCodes):
    """
    This is the base class for a process client. A process client is code that
    executes in the process space of a calling process. If no calling process
    is given, a local one is created on the fly.
    """
    def __init__(self, proc=None, **kwargs):
        """
        Initializes a process client base
        @param proc a IProcess instance as originator of messages
        """
        if not proc:
            proc = Process()
        self.proc = proc

    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send messages from
        """
        if not self.proc.is_spawned():
            yield self.proc.spawn()

    @defer.inlineCallbacks
    def attach(self):
        yield self._check_init()

class ProcessClient(ProcessClientBase):
    """
    This specific derivation adds some glue to interact with a specific targer process.
    """
    def __init__(self, proc=None, target=None, targetname=None, **kwargs):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param target  global scoped (process id or name) to send to
        @param targetname  system scoped exchange name to send messages to
        """
        ProcessClientBase.__init__(self, proc=proc, **kwargs)
        self.target = target
        if not self.target:
            self.target = self.proc.get_scoped_name('system', targetname)

    def rpc_send(self, *args, **kwargs):
        """
        Sends an RPC message to the specified target via originator process
        """
        return self.proc.rpc_send(self.target, *args, **kwargs)

    def rpc_send_protected(self, operation, content, user_id='ANONYMOUS', expiry='0', **kwargs):
        """
        Sends an RPC message to the specified target via originator process
        """

        # Validate expiry value
        assert type(expiry) is str, 'Expiry must be string representation of int time value'

        try:
            expiryval = int(expiry)
        except ValueError, ex:
            assert False, 'Expiry must be string representation of int time value'

        headers = {'user-id':user_id, 'expiry':expiry}
        return self.proc.rpc_send(self.target, operation, content, headers, **kwargs)

    def send(self, *args, **kwargs):
        """
        Sends a message to the specified target via originator process
        """
        return self.proc.send(self.target, *args, **kwargs)

    def reply(self, *args, **kwargs):
        """
        Replies to a message via the originator process
        """
        return self.proc.reply(*args, **kwargs)

# ============================================================================

class ProcessDesc(BasicLifecycleObject):
    """
    Class that encapsulates attributes about a spawnable process; can spawn
    and init processes.
    """
    def __init__(self, **kwargs):
        """
        Initializes ProcessDesc instance with process attributes.
        Also acts as a weak proxy object for use by the parent process.
        @param name  name label of process
        @param module  module name of process module
        @param class  or procclass is class name in process module (optional)
        @param node  ID of container to spawn process on (optional)
        @param spawnargs  dict of additional spawn arguments (optional)
        """
        BasicLifecycleObject.__init__(self)
        self.proc_name = kwargs.get('name', None)
        self.proc_module = kwargs.get('module', None)
        self.proc_class = kwargs.get('class', kwargs.get('procclass', None))
        self.proc_node = kwargs.get('node', None)
        self.spawn_args = kwargs.get('spawnargs', None)
        self.proc_id = None
        self.no_activate = False

    # Life cycle

    @defer.inlineCallbacks
    def spawn(self, parent=None, container=None, activate=True):
        """
        Boilerplate for initialize()
        @param parent the process instance that should be set as supervisor
        """
        #log.info('Spawning name=%s on node=%s' %
        #             (self.proc_name, self.proc_node))
        self.sup_process = parent
        self.container = container or ioninit.container_instance
        pid = yield self.initialize(activate)
        if activate:
            self.no_activate = activate
            yield self.activate()
        defer.returnValue(pid)

    @defer.inlineCallbacks
    def on_initialize(self, activate=False, *args, **kwargs):
        """
        Spawns this process description with the initialized attributes.
        @retval Deferred -> Id with process id
        """
        # Note: If this fails, an exception will occur and be passed through
        self.proc_id = yield self.container.spawn_process(
                procdesc=self,
                parent=self.sup_process,
                node=self.proc_node,
                activate=activate)

        log.info("Process %s ID: %s" % (self.proc_class, self.proc_id))

        defer.returnValue(self.proc_id)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        if self.no_activate:
            self.no_activate = True
        else:
            headers = yield self.container.activate_process(parent=self.sup_process, pid=self.proc_id)

    def shutdown(self):
        return self.terminate()

    @defer.inlineCallbacks
    def on_terminate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        headers = yield self.container.terminate_process(parent=self.sup_process, pid=self.proc_id)

    def on_error(self, cause=None, *args, **kwargs):
        if cause:
            log.error("ProcessDesc error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal state change for ProcessDesc")


class IProcessFactory(Interface):

    def build(spawnargs, container):
        """
        """

class ProcessFactory(object):
    """
    This protocol factory returns receiver instances used to spawn processes
    from a module. This implementation creates process class instances together
    with the receiver. This is a standard implementation that can be used
    in the code of every module containing a process. This factory also collects
    process declarations alongside.
    """
    implements(IProcessFactory)

    receiver = ProcessReceiver

    def __init__(self, pcls, name=None, args=None):
        self.process_class = pcls
        self.name = name or pcls.__name__
        self.args = args or {}

        if self.process_class and not IProcess.implementedBy(self.process_class):
            raise RuntimeError("Class does not implement IProcess")

        # Collecting the declare static class variable in a process class
        if pcls and hasattr(pcls, 'declare') and type(pcls.declare) is dict:
            procdec = pcls.declare.copy()
            procdec['class'] = pcls
            procname = pcls.declare.get('name', pcls.__name__)
            if procname in processes:
                raise RuntimeError('Process already declared: '+str(procname))
            processes[procname] = procdec

    def build(self, spawnargs=None, container=None):
        """
        Factory method to return a process instance from given arguments.
        """
        spawnargs = spawnargs or {}
        container = container or ioninit.container_instance

        #log.debug("ProcessFactory.build(name=%s, args=%s)" % (self.name,spawnargs))

        # Create a process receiver
        procname = spawnargs.get('proc-name', self.name)
        procid = spawnargs.get('proc-id', self.name)
        spawnargs['proc-group'] = self.name

        # Instantiate the IProcess class
        process = self.process_class(spawnargs=spawnargs)

        return process

class ProcessInstantiator(object):
    """
    Instantiator for IProcess instances. Relies on an IProcessFactory
    to instantiate IProcess instances from modules.
    """

    idcount = 0

    @classmethod
    def create_process_id(cls, container=None):
        container = container or ioninit.container_instance
        cls.idcount += 1
        if container:
            containerid = container.id
        else:
            # Purely for tests to avoid a _start_container() in setUp()
            containerid = "TEST-CONTAINER-ID"
        return Id(cls.idcount, containerid)

    @classmethod
    @defer.inlineCallbacks
    def spawn_from_module(cls, module, space=None, spawnargs=None, container=None, activate=True):
        """
        @brief Factory method to spawn a Process instance from a Python module.
                By default, spawn includes an activate
        @param module A module (<type 'module'>) with a ProcessFactory factory
        @param space MessageSpace instance
        @param spawnargs argument dict given to the factory on spawn
        @retval Deferred which fires with the IProcess instance
        """
        spawnargs = spawnargs or {}
        container = container or ioninit.container_instance

        if not hasattr(module, 'factory'):
            raise RuntimeError("Must define factory in process module to spawn")

        if not IProcessFactory.providedBy(module.factory):
            raise RuntimeError("Process model factory must provide IProcessFactory")

        procid = ProcessInstantiator.create_process_id(container)
        spawnargs['proc-id'] = procid.full

        process = yield defer.maybeDeferred(module.factory.build, spawnargs)
        if not IProcess.providedBy(process):
            raise RuntimeError("ProcessFactory returned non-IProcess instance")

        # Give a callback to the process to initialize and activate (if desired)
        try:
            yield process.initialize()
            if activate:
                yield process.activate()
        except Exception, ex:
            log.exception("Error spawning process from module")
            raise ex

        defer.returnValue(process)

# Spawn of the process using the module name
factory = ProcessFactory(Process)
