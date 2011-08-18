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

import StringIO

import logging
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ApplicationError
from ion.core.id import Id
from ion.core.intercept.interceptor import Interceptor
from ion.core.messaging.receiver import ProcessReceiver
from ion.core.messaging.message_client import MessageClient, MessageInstance

from ion.core.process.cprocess import Invocation
from ion.core.data.store import Store
from ion.interact.conversation import ProcessConversationManager, CONV_TYPE_NONE
from ion.interact.request import RequestType
from ion.interact.rpc import RpcType
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject, BasicStates

from ion.core.object import workbench

# despite being from services.dm this is safe - events should be moved to core sometime soon! @TODO
from ion.services.dm.distribution.events import ProcessLifecycleEventPublisher

# Conversation Context object - used to manage workbench GC and identity...
from ion.util.context import ContextObject, ConversationContext

CONF = ioninit.config(__name__)
CF_fail_fast = CONF['fail_fast']
CF_rpc_timeout = CONF['rpc_timeout']
EMPTY_LIST = []

# @todo CHANGE: Dict of "name" to process (service) declaration
processes = {}

# @todo CHANGE: Static store (kvs) to register process instances with names
procRegistry = Store()
procRegistry.kvs = {} # Give this instance its own backend...


class IProcess(Interface):
    """
    Interface for all capability container application processes
    """

class ProcessError(Exception):
    """
    An exception class for errors that occur in Process
    """

# @todo do NOT fill the process namespace with response codes; remove mixin!
class Process(BasicLifecycleObject):
    """
    This is the base class for all processes. Processes can be spawned and
    have a unique identifier. Each process has one main process receiver and can
    define additional receivers as needed. This base class provides a lot of
    mechanics for processes, such as sending and receiving messages, RPC style
    calls, spawning and terminating child processes. Subclasses may use the
    plc-* process life cycle events.
    """
    implements(IProcess)


    """
    Define some constants used in messaging:
    """
    MSG_STATUS = 'status'
    MSG_RESULT = 'result'
    MSG_RESPONSE = 'response'
    MSG_EXCEPTION = 'exception'

    """
    Only used by process.py for uncaught exceptions. Do not catch generic exceptions
    in an ION service - only those which are expected. The Error status and reply error
    are intended only to deal with fatal - unexpected errors.
    """
    ION_ERROR = 'ERROR'
    """
    Generic OK message added
    """
    ION_OK = 'OK'

    BAD_REQUEST = 400
    UNAUTHORIZED = 401


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
        cache_size = int(spawnargs.get('cache_size', 10**7))
        self.workbench = workbench.WorkBench(self, cache_size=cache_size)

        # Create a message Client
        self.message_client = MessageClient(proc=self)

        # A list of other kinds of life cycle objects which are tied to the process
        self._registered_life_cycle_objects = []

        # TCP Connectors and Listening Ports
        self.connectors = []
        self.listeners = []
        
        # publisher for lifecycle change notifications
        self._plcc_pub = ProcessLifecycleEventPublisher(origin=self.id.full, process=self)
        self.add_life_cycle_object(self._plcc_pub)

        # Callbacks before and after ops (handy for unittests)
        self.op_cbs_before = {}
        self.op_cbs_after = {}

        # Context default dictionary
        self.context = ContextObject()
        self._last_context = None

        self.conversation_context = ConversationContext()

        log.debug("NEW Process instance [%s]: id=%s, sup-id=%s, sys-name=%s" % (
                self.proc_name, self.id, self.proc_supid, self.sys_name))

    def _sanitize_opname(self, opname):
        if opname.startswith('op_'): opname = opname[3:]
        return opname

    def add_op_callback_before(self, opname, cb):
        """ Register a callback for before this operation is invoked. Supports "op_thing" and "thing" syntax. """
        self.op_cbs_before.setdefault(self._sanitize_opname(opname), []).append(cb)

    def add_op_callback_after(self, opname, cb):
        """ Register a callback for after this operation is invoked. Supports "op_thing" and "thing" syntax. """
        self.op_cbs_after.setdefault(self._sanitize_opname(opname), []).append(cb)

    def remove_op_callback_before(self, opname, cb):
        self.op_cbs_before.setdefault(self._sanitize_opname(opname), []).remove(cb)

    def remove_op_callback_after(self, opname, cb):
        self.op_cbs_after.setdefault(self._sanitize_opname(opname), []).remove(cb)

    def defer_next_op(self, opname):
        """ Get a deferred that will callback when the given op completes the next time (once). """

        op_d = defer.Deferred()
        def callback(cb_opname, content, payload, msg, result):
            log.info('in callback for defer_next_op')
            op_d.callback(result)
            self.remove_op_callback_after(opname, callback)

        self.add_op_callback_after(opname, callback)
        return op_d

    # --- Life cycle management
    # Categories:
    # op_XXX Message incoming interface
    # spawn, init: Boilerplate API
    # initialize, activate, deactivate, terminate: (Super class) State management API
    # on_XXX: State management API action callbacks
    # plc_XXX: Callback hooks for subclass processes

    def workbench_memory(self):
        """
        A debug method - used in the shell to print the foot print of the workbench
        """
        return "Cached Structure Elements - %d, Cached Repositories - %d, Working Repositories - %d, Memory - %d kb" % \
            (len(self.workbench._workbench_cache), len(self.workbench._repo_cache),len(self.workbench._repos), self.workbench._repo_cache.total_size/1000)

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

        # advance the registered objects as if this process has already transitioned to the READY state,
        # which it will do after this on_initialize method completes
        yield self._advance_life_cycle_objects(BasicStates.S_READY)

        # get length of all registered lcos
        pre_init_lco_len = len(self._registered_life_cycle_objects)

        # Callback to subclasses
        try:
            #import pdb; pdb.set_trace()
            yield defer.maybeDeferred(self.plc_init)
            log.info('Process [%s] id=%s: INIT OK' % (self.proc_name, self.id))
        except Exception, ex:
            log.exception('----- Process %s INIT ERROR -----' % (self.id))
            raise ex

        if len(self._registered_life_cycle_objects) > pre_init_lco_len:
            log.debug("NEW LCOS ADDED DURING INIT")
            yield self._advance_life_cycle_objects(BasicStates.S_READY)

        # publish initialize -> ready transition
        yield self._plcc_pub.create_and_publish_event(state=self._plcc_pub.State.READY)

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

        # advance the registered objects as if this process has already transitioned to the ACTIVE state,
        # which it will do after this on_activate method completes
        yield self._advance_life_cycle_objects(BasicStates.S_ACTIVE)

        # get length of all registered lcos
        pre_active_lco_len = len(self._registered_life_cycle_objects)

        # Callback to subclasses
        try:
            yield defer.maybeDeferred(self.plc_activate)
        except Exception, ex:
            log.exception('----- Process %s ACTIVATE ERROR -----' % (self.id))
            raise ex

        if len(self._registered_life_cycle_objects) > pre_active_lco_len:
            log.debug("NEW LCOS ADDED DURING ACTIVE")
            yield self._advance_life_cycle_objects(BasicStates.S_ACTIVE)

        # publish ready -> active transition
        yield self._plcc_pub.create_and_publish_event(state=self._plcc_pub.State.ACTIVE)

        # last step in activation - cleanup!
        log.debug('Process activation complete - clearing workbench:\n%s' % str(self.workbench))

        self.workbench.manage_workbench_cache('Default Context')


    def plc_activate(self):
        """
        Process life cycle event: on activate of process. Subclasses override.
        """

    def shutdown(self):
        log.debug("[%s] shutdown()" % self.proc_name)
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

        # @todo There should be nothing after the terminate call
        yield self.backend_receiver.deactivate()
        yield self.backend_receiver._await_message_processing()

    @defer.inlineCallbacks
    def on_terminate(self, msg=None, *args, **kwargs):
        """
        @retval Deferred
        """
        # publish active -> terminate transition
        yield self._plcc_pub.create_and_publish_event(state=self._plcc_pub.State.TERMINATED)

        # Clean up all TCP connections and listening ports
        for connector in self.connectors:
            # XXX What is the best way to unit test this?
            connector.disconnect()
        for port in self.listeners:
            yield port.stopListening()

        # advance the registered objects as if this process has already transitioned to the TERMINATED state,
        # which it will do after this on_terminate method completes
        yield self._advance_life_cycle_objects(BasicStates.S_TERMINATED)
        
        # Terminate all child processes
        yield self.shutdown_child_procs()

        yield defer.maybeDeferred(self.plc_terminate)
        log.info('----- Process %s TERMINATED -----' % (self.proc_name))

    def plc_terminate(self):
        """
        Process life cycle event: on termination of process (once)
        """

    @defer.inlineCallbacks
    def on_error(self, cause= None, *args, **kwargs):

        # publish whatever -> error transition - hopefully this works!
        yield self._plcc_pub.create_and_publish_event(state=self._plcc_pub.State.ERROR)

        if len(self._registered_life_cycle_objects) > 0:
            log.debug("Attempting to TERMINATE all registered LCOs")
            # attempt to move all registered lcos to the terminated state cleanly!
            try:
                yield self._advance_life_cycle_objects(BasicStates.S_TERMINATED)
            except Exception:
                log.debug("Error terminating registered LCOs, ignoring...")

        if cause:
            log.error("Process error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal process state change")

        defer.returnValue(None)

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

        self._registered_life_cycle_objects.append(lco)
        yield self._advance_life_cycle_objects()

    @defer.inlineCallbacks
    def _advance_life_cycle_objects(self, curstate=None):
        """
        Helper method to move all registered lifecycle objects in this process to the state
        of either the process or the state passed into this method.

        You can explicitly define the state you want to transition everything to.
        This is used by on_initialize and friends to advance LCOs with a state
        that is just about to occur.
        """

        curstate = curstate or self._get_state()

        states      = [BasicStates.S_INIT,          BasicStates.S_READY,        BasicStates.S_ACTIVE,   BasicStates.S_TERMINATED]
        transitions = [BasicStates.E_INITIALIZE,    BasicStates.E_ACTIVATE,     BasicStates.E_TERMINATE]

        curidx = states.index(curstate)
        log.debug("_advance_lco owning process (%s) is in state %s" % (self.id.full, curstate))

        @defer.inlineCallbacks
        def helper(idx, lco):
            """
            This inline helper methods takes an index and LCO and advances that LCO to match the state in curstate.
            It has an inlineCallbacks decorator to give back a deferred when called so we can wrap that in a deferredList
            to operate on all LCOs in "parallel" - aka not cause an error in one's transitioning to stop transitioning
            LCOs that happen to be later in the registered list.
            """
            lcoidx = states.index(lco._get_state())
            log.debug("_advance_lco cur lco #%d is in state %s" % (idx, lco._get_state()))

            for i in range(lcoidx, curidx):
                input = transitions[i]

                log.debug("_advance_lco cur lco #%d about to put transition %s to %s" % (idx, input, str(lco)))
                try:
                    yield defer.maybeDeferred(lco._so_process, input)

                except Exception, ex:
                    # @TODO: should not be catching this exception.
                    # This should cause the deferred gen'd by inlineCallbacks to errback, which then gets wrapped
                    # nicely by the deferred list. It should not throw an exception in the state object?!?
                    log.debug("Exception occured in transition! Leaving this LCO as is. Ex: %s" % str(ex))
                    break

                log.debug("lco #%d is now at %s" % (idx, lco._get_state()))

            defer.returnValue(None)

        # build deferred list out of calling helper method on all registered LCOs
        dl = defer.DeferredList([helper(idx, lco) for idx,lco in enumerate(self._registered_life_cycle_objects)])
        yield dl

        defer.returnValue(None)


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
            self.context.proc_name = self.proc_name
            # Check if there is a user id in the header, stash if so
            _pre_uid = payload.get('user-id', None)
            _pre_exp = payload.get('expiry', None)
            _action = ''
            if 'user-id' in payload:
                self.context.user_id = payload.get('user-id')
                _action = 'set user_id'
            else:
                log.debug('[%s] receive(): payload anonymous request' % (self.proc_name))
                if self.context.get('user_id', 'Not set') == 'Not set':
                    self.context.user_id = 'ANONYMOUS'
                    _action = 'set ANONYMOUS user_id'
                else:
                    _action = "keep stashed user_id='%s'" % self.context.get('user_id')
            _post_uid = self.context.get('user_id')

            # User session expiry.
            if 'expiry' in payload:
                self.context.expiry = payload.get('expiry')
                _action = _action + '/set expiry'
            else:
                if self.context.get('expiry', 'Not set') == 'Not set':
                    self.context.expiry = '0'
                    _action = _action + '/set 0 expiry'
                else:
                    _action = _action + "/keep stashed expiry='%s'" % self.context.get('expiry')
            _post_exp = self.context.get('expiry')

            log.debug("[%s] receive(): IN:user-id='%s',expiry='%s' ACTION:%s SET:user-id='%s',expiry='%s'" % (
                self.proc_name, _pre_uid, _pre_exp, _action, _post_uid, _post_exp))

            # Extract some headers and make log statement.
            fromname = payload['sender']
            if 'sender-name' in payload:
                fromname = payload['sender-name']   # Legible sender alias
            log.info('>>> [%s] receive(): Message from [%s] ... >>>' % (
                     self.proc_name, fromname))
            convid = payload.get('conv-id', None)
            protocol = payload.get('protocol', None)

            #CONVID is already added to the process.request

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

                # Check some state conditions
                if self._get_state() == BasicStates.S_TERMINATED:
                    if payload.get('op',None) == 'terminate':
                        yield self.reply_ok(msg)
                        defer.returnValue()

                    text = "[%s] Process TERMINATED. Message refused!" % (self.proc_name)
                    log.error(text)
                    raise ProcessError(text)

                elif self._get_state() == BasicStates.S_ERROR:
                    text = "[%s] Process in ERROR state. Message refused!" % (self.proc_name)
                    log.error(text)
                    raise ProcessError(text)

                elif not self._get_state() == "ACTIVE":
                    # Detect and handle request to terminate process.
                    # This does not yet work properly with fail fast...
                    if payload.get('op',None) == 'terminate':
                        yield self.terminate()
                        defer.returnValue()

                    # Non-request messages (e.g. RPC reply are OK before ACTIVE)
                    if payload.get('performative', None) == 'request':
                        text = "[%s] Process in invalid state: '%s'" % (self.proc_name, self._get_state())
                        log.error(text)

                        # @todo: Requeue would be ok, but does not work (Rabbit or client limitation)
                        #d = msg.requeue()

                        # Let the error back handle the exception
                        raise ProcessError(text)

                # Regular message handling in expected state
#                pu.log_message(msg)

                # Delegate further message processing to conversation specific
                # implementation. Trigger conversation FSM.
                # @see ion.interact.rpc, ion.interact.request
                res = yield self.conv_manager.msg_received(message)

                # Problem: what if FSM produces an error (it does not currently)
                # Problem: log message here after reply/failure have been sent
                self.conv_manager.log_conv_message(conv, message, msgtype='RECV')

                self.conv_manager.check_conversation_state(conv)

            elif convid and protocol == CONV_TYPE_NONE:
                # Case of one-off messages
                log.debug("Received simple protocol=='none' message")
#                pu.log_message(msg)
                res = yield self._dispatch_message_op(payload, msg, None)

            else:
                # Legacy case of no conv-id set or one-off messages (events?)
                log.warn("No conversation id in message")
#                pu.log_message(msg)
                res = yield self._dispatch_message_op(payload, msg, None)

        except ApplicationError, ex:
            # In case of an application error - do not terminate the process!
            if log.getEffectiveLevel() <= logging.INFO:    # only output all this stuff when debugging
                log.exception("*****Non Conversation Application error in message processing*****")
                log.error('*** Message Payload which cause the error: \n%s' % pu.pprint_to_string(payload))
                log.error('*** Message Content: \n%s' % str(payload.get('content', '## No Content! ##')))
                log.error("*****End Non Conversation Application error in message processing*****")

            # @todo Should we send an err or rather reject the msg?
            # @note We can only send a reply_err to an RPC
            if msg and msg.payload['reply-to'] and msg.payload.get('performative',None)=='request':
                yield self.reply_err(msg, exception = ex)

        except Exception, ex:
            # *** PROBLEM. Here the conversation is in ERROR state

            log.exception("*****Non Conversation Application error in message processing*****")
            log.error('*** Message Payload which cause the error: \n%s' % pu.pprint_to_string(payload))
            if log.getEffectiveLevel() <= logging.WARN:
                log.error('*** Message Content: \n%s' % str(payload.get('content', '## No Content! ##')))
            log.error("*****End Non Conversation Application error in message processing*****")

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

            cb_opname = self._sanitize_opname(opname)
            for cb in self.op_cbs_before.get(cb_opname, EMPTY_LIST):
                cb(cb_opname, content, payload, msg)

            result = yield defer.maybeDeferred(opf, content, payload, msg)

            for cb in self.op_cbs_after.get(cb_opname, EMPTY_LIST):
                cb(cb_opname, content, payload, msg, result)
        elif hasattr(self,'op_none'):
            yield defer.maybeDeferred(self.op_none, content, payload, msg)
        else:
            # Change to Raise?
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

        log.debug("[%s] request(): NEW conversation type=%s as initiator -> participant=%s" % (
                self.proc_name, rpc_conv.protocol, recv))

        if headers is None:
            headers = {}
        headers['protocol'] = rpc_conv.protocol
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

        log.debug("[%s] request(): NEW conversation type=%s as initiator -> participant=%s" % (
                self.proc_name, req_conv.protocol, receiver))

        if headers is None:
            headers = {}
        headers['protocol'] = req_conv.protocol
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
        assert conv, "Conversation instance must exist for blocking send"

        # Create a new deferred that the caller can yield on to wait for RPC
        conv.blocking_deferred = defer.Deferred()
        # Timeout handling
        timeout = float(kwargs.get('timeout', CF_rpc_timeout))
        def _timeoutf():
            log.info('Timeout on blocking send - headers: \n%s' % str(headers))
            log.warn("Process %s RPC conv-id=%s timed out on operation - '%s' ! " % (self.proc_name,conv.conv_id, operation))
            p_headers = pu.pprint_to_string(headers)
            p_content = pu.pprint_to_string(content)

            log.info('Timedout Message Receive: %s' % recv)
            log.info('Timedout Message Headers: %s' % p_headers)
            log.info('Timedout Message Operation: %s' % operation)
            log.info('Timedout Message Content: %s' % p_content)

            # Remove RPC. Delayed result will go to catch operation
            conv.timeout = str(pu.currenttime_ms())
            conv.blocking_deferred.errback(defer.TimeoutError())
        if timeout:
            callto = reactor.callLater(timeout, _timeoutf)
            conv.blocking_deferred.rpc_call = callto

        # Call to send()
        d = self.send(recv=recv,
                      operation=operation,
                      headers=headers,
                      content=content,
                      conv=conv)
        # d is a deferred. The actual send of the request message will happen
        # after this method returns. This is OK, because functions are chained
        # to call back the caller on the rpc_deferred when the receipt is done.

        return conv.blocking_deferred

    @defer.inlineCallbacks
    def send(self, recv, operation, content, headers=None, send_receiver=None, conv=None, quiet=False):
        """
        @brief Send a message via the process receiver to destination.
            Starts a new conversation.
        @retval Deferred for send of message
        """
        msgheaders = {}
        msgheaders['sender-name'] = self.proc_name

        if conv is not None:
            msgheaders['conv-id'] = conv.conv_id

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
            msgheaders['user-id'] = self.context.get('user_id', 'ANONYMOUS')
            log.debug('[%s] send(): set user id in msgheaders from stashed user_id [%s]' % (self.proc_name, msgheaders['user-id']))
        else:
            log.debug('[%s] send(): using user id from msgheaders [%s]' % (self.proc_name, msgheaders['user-id']))
        if not 'expiry' in msgheaders:
            msgheaders['expiry'] = self.context.get('expiry', '0')
            log.debug('[%s] send(): set expiry in msgheaders from stashed expiry [%s]' % (self.proc_name, msgheaders['expiry']))
        else:
            log.debug('[%s] send(): using expiry from msgheaders [%s]' % (self.proc_name, msgheaders['expiry']))

        if quiet:
            msgheaders['quiet'] = True

        # Now allow headers to override any of the precomputed headers
        if headers:
            msgheaders.update(headers)

        # Assemble message content
        if content is None:
            content = yield self.message_client.create_instance(MessageContentTypeID=None)
        if isinstance(content, MessageInstance):
            if not content.Message.IsFieldSet('response_code'):
                content.MessageResponseCode = content.ResponseCodes.OK

        # Assemble the standard in-memory message object
        message = dict(recipient=recv, operation=operation,
                       content=content, headers=msgheaders,
                       performative=msgheaders.get('performative','request'),
                       process=self, conversation=conv)

        # Put the message through the conversation FSM
        res = None
        try:
            res1 = yield self.conv_manager.msg_send(message)

            # PROBLEM: FSM does not raise any exception.

            # FSM processed successfully
            if send_receiver is None:
                # The default case is to send out via the backend receiver
                res2 = yield self.backend_receiver.send(**message)
            else:
                # Use given receiver (e.g. primary receiver for RPC replies)
                res2 = yield send_receiver.send(**message)

            message['headers'] = res2
            self.conv_manager.log_conv_message(conv, message, msgtype='SENT')

            self.conv_manager.check_conversation_state(conv)

            res = res1
        except Exception, ex:
            log.exception("ERROR [%s] send() in FSM - Message not sent" % self.proc_name)
            raise ex

        defer.returnValue(res)

    def reply(self, msg, operation=None, content=None, headers=None, performative=None, quiet=False):
        """
        @brief Replies to a given message, continuing the ongoing conversation
        @retval Deferred for message send
        """
        # The causing message we reply to
        req_msg = msg.payload
        recv = req_msg.get('reply-to', None)
        recv_id = pu.get_process_id(recv)

        # Assemble the reply message
        msgheaders = {}

        msgheaders['protocol'] = req_msg['protocol']

        if recv is None:
            log.error('No reply-to given for message '+str(msg))
        else:
            msgheaders['conv-id'] = req_msg.get('conv-id','')
            msgheaders['conv-seq'] = int(req_msg.get('conv-seq',0)) + 1

        if performative:
            msgheaders['performative'] = performative
        elif 'performative' not in msgheaders:
            msgheaders['performative'] = 'inform_result'

        # Now allow headers to override any of the precomputed headers
        if headers is not None:
            msgheaders.update(headers)

        if operation is None:
            operation = self.MSG_RESULT

        return self.send(recv=recv_id,
                         operation=operation,
                         headers=msgheaders,
                         content=content,
                         send_receiver=self.receiver,
                         quiet=quiet)

    def reply_agree(self, msg, content=None, headers=None):
        """
        @brief Internal boilerplate method that replies to a given message with
            an agree performative
        @retval Deferred for send of reply
        """
        return self.reply(msg,
                          performative='agree',
                          operation=self.MSG_RESULT,
                          headers=headers,
                          content=content,
                          quiet=True)

    def reply_refuse(self, msg, content=None, headers=None):
        """
        @brief Internal boilerplate method that replies to a given message with
            a refuse performative
        @retval Deferred for send of reply
        """
        return self.reply(msg,
                          performative='refuse',
                          operation=self.MSG_RESULT,
                          headers=headers,
                          content=content)

    def reply_ok(self, msg, content=None, headers=None):
        """
        @brief Internal boilerplate method that replies to a given message with
            an inform-result performative
        @retval Deferred for send of reply
        """
        return self.reply(msg,
                          performative='inform_result',
                          operation=self.MSG_RESULT,
                          headers=headers,
                          content=content,
                          quiet=(content is None))

    @defer.inlineCallbacks
    def reply_err(self, msg, content=None, headers=None, exception=None, response_code=None):
        """
        @brief Internal boilerplate method that replies to a given message with
            a failure performative. Will result at the receiver end in an
            application level error. The result can include content, a caught
            exception and an application level error_code as an indication of the error.
        @content Message content object
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

        msgheaders = {}
        msgheaders[self.MSG_STATUS] = self.ION_ERROR

        if headers != None:
            msgheaders.update(headers)

        res = yield self.reply(msg,
                               performative='failure',
                               operation=self.MSG_RESULT,
                               headers=msgheaders,
                               content=content)
        defer.returnValue(res)

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

    @defer.inlineCallbacks
    def shutdown_child_procs(self):
        if len(self.child_procs) > 0:
            log.info("Shutting down %s child processes" % (len(self.child_procs)))
        while len(self.child_procs) > 0:
            child = self.child_procs.pop()
            try:
                res = yield self.shutdown_child(child)
            except Exception, ex:
                log.exception("Error terminating child %s" % child.proc_id)


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


class ProcessClientBase(object):
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
            procname = kwargs.get('proc-name', self.__class__.__name__ + "_aproc")
            proc = Process(spawnargs={'proc-name':procname})
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
