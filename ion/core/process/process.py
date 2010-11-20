#!/usr/bin/env python

"""
@file ion/core/process/process.py
@author Michael Meisinger
@brief base classes for processes within a capability container
"""

from twisted.internet import defer, reactor
from twisted.python import failure
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ReceivedError
from ion.core.id import Id
from ion.core.intercept.interceptor import Interceptor
from ion.core.messaging.receiver import ProcessReceiver
from ion.core.messaging.ion_reply_codes import ResponseCodes
from ion.core.process.cprocess import IContainerProcess, ContainerProcess
from ion.services.dm.preservation.store import Store
from ion.interact.conversation import Conversation
from ion.interact.message import Message
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject

from ion.core.object import workbench

CONF = ioninit.config(__name__)
CF_conversation_log = CONF['conversation_log']
CF_fail_fast = CONF['fail_fast']
CF_rpc_timeout = CONF['rpc_timeout']

# @todo CHANGE: Dict of "name" to process (service) declaration
processes = {}

# @todo CHANGE: Static store (kvs) to register process instances with names
procRegistry = Store()

class IProcess(Interface):
    """
    Interface for all capability container application processes
    """

class Process(BasicLifecycleObject,ResponseCodes):
    """
    This is the base class for all processes. Processes can be spawned and
    have a unique identifier. Each process has one main process receiver and can
    define additional receivers as needed. This base class provides a lot of
    mechanics for processes, such as sending and receiving messages, RPC style
    calls, spawning and terminating child processes. Subclasses may use the
    plc-* process life cycle events.
    """
    implements(IProcess)

    # @todo CHANGE: Conversation ID counter
    convIdCnt = 0
    
    
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
                                    handler=self.receive)

        # Create a backend receiver for outgoing RPC process interactions.
        # Needed to avoid deadlock when processing incoming messages
        # because only one message can be consumed before ACK.
        self.backend_id = Id(self.id.local+"b", self.id.container)
        self.backend_receiver = ProcessReceiver(
                                    label=self.proc_name,
                                    name=self.backend_id.full,
                                    group=self.proc_group,
                                    process=self,
                                    handler=self.receive)

        # Dict of all receivers of this process. Key is the name
        self.receivers = {}
        self.add_receiver(self.receiver)
        self.add_receiver(self.backend_receiver)

        # Dict of converations by conv-id
        self.conversations = {}

        # Conversations by conv-id for currently outstanding RPCs
        self.rpc_conv = {}

        # List of ProcessDesc instances of defined and spawned child processes
        self.child_procs = []

        #The Workbench for all object repositories used by this process
        self.workbench = workbench.WorkBench(self)
        
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
        Life cycle callback for the initialization "spawn" of the process.
        @retval Deferred for the Id of the process (self.id)
        """
        assert not self.backend_receiver.consumer, "Process already initialized"
        log.debug('Process [%s] id=%s initialize()' % (self.proc_name, self.id))

        # Create queue only for process receiver
        yield self.receiver.initialize()

        # Create queue and consumer for backend receiver
        yield self.backend_receiver.initialize()
        yield self.backend_receiver.activate()

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
                Activation has to go through the container (agent)
        """
        try:
            yield self.activate(content, headers, msg)
            if msg != None:
                yield self.reply_ok(msg)
        except Exception, ex:
            if msg != None:
                yield self.reply_uncaught_err(msg, content=None, exception=ex, response_code = "Process %s ACTIVATE ERROR" % (self.id))

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        log.debug('Process [%s] id=%s activate()' % (self.proc_name, self.id))

        # Create consumer for process receiver
        yield self.receiver.activate()

        # Callback to subclasses
        try:
            yield defer.maybeDeferred(self.plc_activate)
        except Exception, ex:
            log.exception('----- Process %s ACTIVATE ERROR -----' % (self.id))
            raise ex

    def plc_activate(self):
        """
        Process life cycle event: on activate of process
        """

    def shutdown(self):
        return self.terminate()

    @defer.inlineCallbacks
    def op_terminate(self, content, headers, msg):
        """
        Shutdown operation, on receive of the init message
        """
        try:
            yield self.terminate()
            if msg != None:
                yield self.reply(msg)
        except Exception, ex:
            if msg != None:
                yield self.reply_uncaught_err(msg, content=None, exception=ex, response_code = "Process %s TERMINATE ERROR" % (self.id))

    @defer.inlineCallbacks
    def on_terminate(self, msg=None, *args, **kwargs):
        """
        @retval Deferred
        """
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

    #    @defer.inlineCallbacks
    def op_sys_procexit(self, content, headers, msg):
        """
        Called when a child process has exited without being terminated. A
        supervisor may process this event and restart the child.
        """
        pass

    # --- Internal helper methods

    def add_receiver(self, receiver):
        self.receivers[receiver.name] = receiver

    def is_spawned(self):
        return self.receiver.consumer != None

    # --- Incoming message handling

    @defer.inlineCallbacks
    def receive(self, payload, msg):
        """
        This is the first and MAIN entry point for received messages. Messages are
        separated into RPC replies (by conversation ID) and other received
        messages.
        """
        try:
            # Check if this response is in reply to an outstanding RPC call
            if 'conv-id' in payload and payload['conv-id'] in self.rpc_conv:
                yield self._receive_rpc(payload, msg)
            else:
                yield self._receive_msg(payload, msg)
        except Exception, ex:
            log.exception('Error in process %s receive ' % self.proc_name)
            if msg and msg.payload['reply-to']:
                yield self.reply_uncaught_err(msg, content=None, exception=ex, response_code=self.ION_RECEIVER_ERROR)

    @defer.inlineCallbacks
    def _receive_rpc(self, payload, msg):
        """
        Handling of RPC reply messages.
        """
        fromname = payload['sender']
        if 'sender-name' in payload:
            fromname = payload['sender-name']
        log.info('>>> [%s] receive(): RPC reply from [%s] <<<' % (self.proc_name, fromname))
        rpc_deferred = self.rpc_conv.pop(payload['conv-id'])
        content = payload.get('content', None)
        if type(rpc_deferred) is str:
            log.error("Message received after process %s RPC conv-id=%s timed out=%s: %s" % (
                self.proc_name, payload['conv-id'], rpc_deferred, payload))
            return
        rpc_deferred.rpc_call.cancel()
        res = (content, payload, msg)


        yield msg.ack()
        
        status = payload.get(self.MSG_STATUS, None)
        if status == self.ION_OK:
            #Cannot do the callback right away, because the message is not yet handled
            reactor.callLater(0, lambda: rpc_deferred.callback(res))
                
        elif status == self.ION_ERROR:
            log.warn('RPC reply is an ERROR: '+str(payload.get(self.MSG_RESPONSE)))
            log.debug('RPC reply ERROR Content: '+str(content))
            err = failure.Failure(ReceivedError(payload, content))
            #rpc_deferred.errback(err)
            # Cannot do the callback right away, because the message is not yet handled
            reactor.callLater(0, lambda: rpc_deferred.errback(err))
            
            
        else:
            log.error('RPC reply is not well formed. Header "status" must be set!')
            #Cannot do the callback right away, because the message is not yet handled
            reactor.callLater(0, lambda: rpc_deferred.callback(res))

    @defer.inlineCallbacks
    def _receive_msg(self, payload, msg):
        """
        Handling of non-RPC messages. Messages are dispatched according to
        message attributes.
        """        
        fromname = payload['sender']
        if 'sender-name' in payload:
            fromname = payload['sender-name']
        log.info('#####>>> [%s] receive(): Message from [%s], dispatching... >>>' % (
                 self.proc_name, fromname))
        convid = payload.get('conv-id', None)
        conv = self.conversations.get(convid, None) if convid else None
        # Perform a dispatch of message by operation
        try:
            res = yield self._dispatch_message(payload, msg, conv)
        except Exception, ex:
            log.exception("*****Error in message processing*****")
            # @todo Should we send an err or rather reject the msg?
            if msg and msg.payload['reply-to']:
                yield self.reply_uncaught_err(msg, content=None, exception = str(ex), response_code=self.ION_RECEIVER_ERROR)

            if CF_fail_fast:
                yield self.terminate()
                # Send exit message to supervisor
        finally:
            # @todo This is late here (potentially after a reply_err before)
            if msg._state == "RECEIVED":
                # Only if msg has not been ack/reject/requeued before
                log.debug("<<< ACK msg")
                yield msg.ack()
                
    @defer.inlineCallbacks
    def _dispatch_message(self, payload, msg, conv):
        """
        Dispatch of messages to operation handler functions  within this
        process instance. The default behavior is to dispatch to 'op_*' functions,
        where * is the 'op' message header.
        @retval Deferred
        """
        if not self._get_state() == "ACTIVE":
            text = "Process %s in invalid state %s." % (self.proc_name, self._get_state())
            log.error(text)

            # @todo: Requeue would be ok, but does not work (Rabbit limitation)
            #d = msg.requeue()
            if msg and msg.payload['reply-to']:
                yield self.reply_uncaught_err(msg, content=None, response_code = text)
            return

        # Regular message handling in expected state
        pu.log_message(msg)

        if "op" in payload:
            op = payload['op']
            content = payload.get('content','')
            opname = 'op_' + str(op)

            # dynamically invoke the operation in the given class
            if hasattr(self, opname):
                opf = getattr(self, opname)
                yield defer.maybeDeferred(opf, content, payload, msg)
            elif hasattr(self,'op_none'):
                yield defer.maybeDeferred(self.op_none, content, payload, msg)
            else:
                log.error("receive() failed. Cannot dispatch to operation")
        else:
            log.error("Invalid message. No 'op' in header", payload)

    def op_none(self, content, headers, msg):
        """
        The method called if operation callback operation is not defined
        """
        log.error('Process does not define op=%s' % headers.get('op',None))

    # --- Outgoing message handling

    def rpc_send(self, recv, operation, content, headers=None, **kwargs):
        """
        @brief Sends a message RPC style and waits for conversation message reply.
        @retval a Deferred with the message value on receipt
        """
        msgheaders = self._prepare_message(headers)
        convid = msgheaders['conv-id']
        # Create a new deferred that the caller can yield on to wait for RPC
        rpc_deferred = defer.Deferred()
        # Timeout handling
        timeout = float(kwargs.get('timeout', CF_rpc_timeout))
        def _timeoutf():
            log.warn("Process %s RPC conv-id=%s timed out! " % (self.proc_name,convid))
            # Remove RPC. Delayed result will go to catch operation
            d = self.rpc_conv.pop(convid)
            self.rpc_conv[convid] = "TIMEOUT:%s" % pu.currenttime_ms()
            d.errback(defer.TimeoutError())
        if timeout:
            callto = reactor.callLater(timeout, _timeoutf)
            rpc_deferred.rpc_call = callto
        self.rpc_conv[convid] = rpc_deferred
        d = self.send(recv, operation, content, msgheaders)
        # d is a deferred. The actual send of the request message will happen
        # after this method returns. This is OK, because functions are chained
        # to call back the caller on the rpc_deferred when the receipt is done.
        return rpc_deferred

    def send(self, recv, operation, content, headers=None, reply=False):
        """
        @brief Send a message via the process receiver to destination.
        Starts a new conversation.
        @retval Deferred for send of message
        """
        msgheaders = self._prepare_message(headers)
        message = dict(recipient=recv, operation=operation,
                       content=content, headers=msgheaders)
        if reply:
            d = self.receiver.send(**message)
        else:
            d = self.backend_receiver.send(**message)
        return d

    def _prepare_message(self, headers):
        msgheaders = {}
        msgheaders['sender-name'] = self.proc_name
        if headers:
            msgheaders.update(headers)
        if not 'conv-id' in msgheaders:
            convid = self._create_convid()
            msgheaders['conv-id'] = convid
            msgheaders['conv-seq'] = 1
            self.conversations[convid] = Conversation()
        return msgheaders

    def _create_convid(self):
        # Returns a new unique conversation id
        send = self.id.full
        Process.convIdCnt += 1
        convid = "#" + str(Process.convIdCnt)
        #convid = send + "#" + Process.convIdCnt
        return convid

    def reply(self, msg, operation=None, content=None, response_code='',exception='', headers={}):
        """
        @brief Replies to a given message, continuing the ongoing conversation
        @retval Deferred or None
        """
        if not operation:
            operation = self.MSG_RESULT
                
        ionMsg = msg.payload
        recv = ionMsg.get('reply-to', None)
        if recv == None:
            log.error('No reply-to given for message '+str(msg))
        else:
            headers['conv-id'] = ionMsg.get('conv-id','')
            headers['conv-seq'] = int(ionMsg.get('conv-seq',0)) + 1
            
        # Values in the headers KWarg take precidence over the response_code and exception KWargs!
        reshdrs = dict()
        
        if not response_code:
            response_code = self.ION_SUCCESS
        reshdrs[self.MSG_RESPONSE] = str(response_code)
        reshdrs[self.MSG_EXCEPTION] = str(exception)
        
        # MSG STATUS is set automatically!
        #reshdrs[self.MSG_STATUS] = self.ION_OK
                
        reshdrs.update(headers)
            
        return self.send(pu.get_process_id(recv), operation, content, reshdrs, reply=True)

    def reply_ok(self, msg, content=None, headers={}):
        """
        Boilerplate method that replies to a given message with a success
        message and a given result value
        @content any sendable type to be converted to dict, or dict (untouched)
        @retval Deferred for send of reply
        """
        # Note: Header status=OK is automatically set

        #if not type(content) is dict:
        #    content = dict(value=content, status='OK')
        
        # This is basically a pass through for the reply method interface - only
        # used for backward compatibility!
        log.info('''REPLY_OK is depricated - please use "reply"''')
        
        return self.reply(msg, operation=self.MSG_RESULT, content=content, headers=headers)

    def reply_uncaught_err(self, msg, content=None, response_code='', exception='', headers={}):
        """
        Reply to a message with an uncaught exception using an error message and
        an indication of the error.
        @content any sendable type to be converted to dict, or dict (untouched)
        @exception an instance of Exception
        @response_code a more informative error message
        @retval Deferred for send of reply
        """
        reshdrs = dict()
        reshdrs[self.MSG_STATUS] = str(self.ION_ERROR)
        reshdrs[self.MSG_RESPONSE] = str(response_code)
        reshdrs[self.MSG_EXCEPTION] = str(exception)
        
        reshdrs.update(headers)
            
        return self.reply(msg, content=content, headers=reshdrs)
        
        
    #def reply_err(self, msg, content=None, headers=None, exception='', response_code=''):
    #    """
    #    Boilerplate method for reply to a message which lead to an application
    #    level error. The result can include content, a caught exception and an
    #    application level error_code as an indication of the error.
    #    @content any sendable type to be converted to dict, or dict (untouched)
    #    @exception an instance of Exception
    #    @response_code an ION application level defined error code for a handled exception
    #    @retval Deferred for send of reply
    #    """
    #    reshdrs = dict()
    #    # The status is still OK - this is for handled exceptions!
    #    reshdrs[self.MSG_STATUS] = str(self.ION_OK)
    #    reshdrs[self.MSG_APP_ERROR] = str(response_code)
    #    reshdrs[self.MSG_EXCEPTION] = str(exception)
    #    
    #    if headers != None:
    #        reshdrs.update(headers)
    #        
    #    return self.reply(msg, self.MSG_RESULT, content, reshdrs)

    def get_conversation(self, headers):
        convid = headers.get('conv-id', None)
        return self.conversations(convid, None)

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

class ProcessClient(object,ResponseCodes):
    """
    This is the base class for a process client. A process client is code that
    executes in the process space of a calling process. If no calling process
    is given, a local one is created on the fly. This client adds some
    glue to interact with a specific targer process
    """
    def __init__(self, proc=None, target=None, targetname=None, **kwargs):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param target  global scoped (process id or name) to send to
        @param targetname  system scoped exchange name to send messages to
        """
        if not proc:
            proc = Process()
        self.proc = proc
        assert target or targetname, "Need either target or targetname"
        self.target = target
        if not self.target:
            self.target = self.proc.get_scoped_name('system', targetname)

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

    def rpc_send(self, *args):
        """
        Sends an RPC message to the specified target via originator process
        """
        return self.proc.rpc_send(self.target, *args)

    def send(self, *args):
        """
        Sends a message to the specified target via originator process
        """
        return self.proc.send(self.target, *args)

    def reply(self, *args):
        """
        Replies to a message via the originator process
        """
        return self.proc.reply(*args)

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
