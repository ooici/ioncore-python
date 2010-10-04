#!/usr/bin/env python

"""
@file ion/core/base_process.py
@author Michael Meisinger
@brief base class for all processes within a capability container
"""

from twisted.internet import defer
from zope.interface import implements

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.id import Id
from ion.core import ioninit
from ion.core.messaging.receiver import ProcessReceiver
from ion.core.process.process import IProcess, ProcessDesc, ProcessFactory
from ion.core.process.process import ProcessInstantiator
from ion.data.store import Store
from ion.interact.conversation import Conversation
from ion.interact.message import Message
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject

CONF = ioninit.config(__name__)
CF_conversation_log = CONF['conversation_log']

# @todo CHANGE: Static store (kvs) to register process instances with names
procRegistry = Store()

class BaseProcess(BasicLifecycleObject):
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
        log.debug('Process id=%s initialize()' % (self.id))

        # Create queue only for process receiver
        yield self.receiver.initialize()

        # Create queue and consumer for backend receiver
        yield self.backend_receiver.initialize()
        yield self.backend_receiver.activate()

        # Callback to subclasses
        try:
            yield defer.maybeDeferred(self.plc_init)
            log.info('Process id=%s [%s]: INIT OK' % (self.id, self.proc_name))
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
                yield self.reply_err(msg, "Process %s ACTIVATE ERROR" % (self.id), exception=ex)

    @defer.inlineCallbacks
    def on_activate(self, *args, **kwargs):
        """
        @retval Deferred
        """
        log.debug('Process id=%s activate()' % (self.id))

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
                yield self.reply_ok(msg)
        except Exception, ex:
            if msg != None:
                yield self.reply_err(msg, "Process %s TERMINATE ERROR" % (self.id), exception=ex)

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

        yield defer.maybeDeferred(self.plc_shutdown)
        log.info('----- Process %s TERMINATED -----' % (self.proc_name))

    def plc_shutdown(self):
        """
        Process life cycle event: on shutdown of process (once)
        """

    def on_error(self, cause= None, *args, **kwargs):
        if cause:
            log.error("BaseProcess error: %s" % cause)
            pass
        else:
            raise RuntimeError("Illegal process state change")

    # --- Internal helper methods

    def add_receiver(self, receiver):
        self.receivers[receiver.name] = receiver

    def is_spawned(self):
        return self.receiver.consumer != None

    # --- Incoming message handling

    def receive(self, payload, msg):
        """
        This is the first and MAIN entry point for received messages. Messages are
        separated into RPC replies (by conversation ID) and other received
        messages.
        """
        try:
            # Check if this response is in reply to an outstanding RPC call
            if 'conv-id' in payload and payload['conv-id'] in self.rpc_conv:
                d = self._receive_rpc(payload, msg)
            else:
                d = self._receive_msg(payload, msg)
        except Exception, ex:
            # Unexpected error condition in message processing (only before
            # any callback is called)
            log.exception('Error in process %s receive ' % self.proc_name)
            # @todo: There was an error and now what??
            if msg and msg.payload['reply-to']:
                d = self.reply_err(msg, 'ERROR in process receive()', exception=ex)

    def _receive_rpc(self, payload, msg):
        """
        Handling of RPC reply messages.
        @todo: Handle the error case
        """
        fromname = payload['sender']
        if 'sender-name' in payload:
            fromname = payload['sender-name']
        log.info('>>> [%s] receive(): RPC reply from [%s] <<<' % (self.proc_name, fromname))
        d = self.rpc_conv.pop(payload['conv-id'])
        content = payload.get('content', None)
        res = (content, payload, msg)
        if type(content) is dict and content.get('status',None) == 'OK':
            pass
        elif type(content) is dict and content.get('status',None) == 'ERROR':
            log.warn('RPC reply is an ERROR: '+str(content.get('value',None)))
        else:
            log.error('RPC reply is not well formed. Use reply_ok or reply_err')
        # @todo is it OK to ack the response at this point already?
        d1 = msg.ack()
        if d1:
            d1.addCallback(lambda res1: d.callback(res))
            d1.addErrback(lambda c: d.errback(c))
            return d1
        else:
            # Support for older carrot version where ack did not return deferred
            d.callback(res)
            return d

    def _receive_msg(self, payload, msg):
        """
        Handling of non-RPC messages. Messages are dispatched according to
        message attributes.
        """
        fromname = payload['sender']
        if 'sender-name' in payload:
            fromname = payload['sender-name']
        log.info('#####>>> [%s] receive(): Message from [%s], dispatching... >>>' % (self.proc_name, fromname))
        convid = payload.get('conv-id', None)
        conv = self.conversations.get(convid, None) if convid else None
        # Perform a dispatch of message by operation
        # @todo: Handle failure case. Message reject?
        d = self._dispatch_message(payload, msg, self, conv)
        def _cb(res):
            if msg._state == "RECEIVED":
                # Only if msg has not been ack/reject/requeued before
                log.debug("<<< ACK msg")
                d1 = msg.ack()
        def _err(res):
            log.error("*****Error in message processing: "+str(res)+"*****")
            if msg._state == "RECEIVED":
                # Only if msg has not been ack/reject/requeued before
                log.debug("<<< ACK msg")
                d1 = msg.ack()
            # @todo Should we send an err or rather reject the msg?
            if msg and msg.payload['reply-to']:
                d2 = self.reply_err(msg, 'ERROR in process receive(): '+str(res))
        d.addCallbacks(_cb, _err)
        return d

    def _dispatch_message(self, payload, msg, target, conv):
        """
        Dispatch of messages to operations within this process instance. The
        default behavior is to dispatch to 'op_*' functions, where * is the
        'op' message attribute.
        @retval Deferred
        """
        if self._get_state() == "ACTIVE":
            # Regular message handling in expected state
            d = pu.dispatch_message(payload, msg, target, conv)
            return d
        else:
            text = "Process %s in invalid state %s." % (self.proc_name, self._get_state())
            log.error(text)

            # @todo: Requeue would be ok, but does not work (Rabbit limitation)
            #d = msg.requeue()
            if msg and msg.payload['reply-to']:
                d = self.reply_err(msg, text)
            if not d:
                d = defer.succeed(None)
            return d

    def op_none(self, content, headers, msg):
        """
        The method called if operation callback operation is not defined
        """
        log.info('Catch message op=%s' % headers.get('op',None))

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
        timeout = float(kwargs.get('timeout',0))
        def _timeoutf(d, convid, *args, **kwargs):
            log.info("RPC on conversation %s timed out! "%(convid))
            # Remove RPC. Delayed result will go to catch operation
            d = self.rpc_conv.pop(convid)
            d.errback(defer.TimeoutError())
        if timeout:
            rpc_deferred.setTimeout(timeout, _timeoutf, convid)
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
        if reply:
            send = self.id
        else:
            send = self.backend_id

        return pu.send(None, send, recv, operation, content, msgheaders)

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
        BaseProcess.convIdCnt += 1
        convid = "#" + str(BaseProcess.convIdCnt)
        #convid = send + "#" + BaseProcess.convIdCnt
        return convid

    def reply(self, msg, operation, content, headers=None):
        """
        @brief Replies to a given message, continuing the ongoing conversation
        @retval Deferred or None
        """
        ionMsg = msg.payload
        recv = ionMsg.get('reply-to', None)
        if not headers:
            headers = {}
        if recv == None:
            log.error('No reply-to given for message '+str(msg))
        else:
            headers['conv-id'] = ionMsg.get('conv-id','')
            headers['conv-seq'] = int(ionMsg.get('conv-seq',0)) + 1
            return self.send(pu.get_process_id(recv), operation, content, headers, reply=True)

    def reply_ok(self, msg, content=None, headers=None):
        """
        Boilerplate method that replies to a given message with a success
        message and a given result value
        @content any sendable type to be converted to dict, or dict (untouched)
        @retval Deferred for send of reply
        """
        # Note: Header status=OK is automatically set
        if not type(content) is dict:
            content = dict(value=content, status='OK')
        return self.reply(msg, 'result', content, headers)

    def reply_err(self, msg, content=None, headers=None, exception=None):
        """
        Boilerplate method for reply to a message with an error message and
        an indication of the error.
        @content any sendable type to be converted to dict, or dict (untouched)
        @exception an instance of Exception
        @retval Deferred for send of reply
        """
        reshdrs = dict(status='ERROR')
        if headers != None:
            reshdrs.update(headers)
        if not type(content) is dict:
            content = dict(value=content, status='ERROR')
            if exception:
                # @todo Add more info from exception
                content['errmsg'] = str(exception)
        return self.reply(msg, 'result', content, reshdrs)

    def get_conversation(self, headers):
        convid = headers.get('conv-id', None)
        return self.conversations(convid, None)

    # --- Process and child process management

    def get_scoped_name(self, scope, name):
        """
        Returns a name that is scoped. Local=Name prefixed by container id.
        System=Name prefixed by system name, ie id of root process's container.
        Global=Name unchanged.
        @param scope  one of "local", "system" or "global"
        @param name name to be scoped
        """
        scoped_name = name
        if scope == 'local':
            scoped_name =  str(Id.default_container_id) + "." + name
        elif scope == 'system':
            scoped_name =  self.sys_name + "." + name
        elif scope == 'global':
            pass
        else:
            assert 0, "Unknown scope: "+scope
        return  scoped_name

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


# Spawn of the process using the module name
factory = ProcessFactory(BaseProcess)

class BaseProcessClient(object):
    """
    This is the base class for a process client. A process client is code that
    executes in the process space of a calling process. If no calling process
    is given, a local one is created on the fly. This client adds some
    glue to interact with a specific targer process
    """
    def __init__(self, proc=None, target=None, targetname=None, **kwargs):
        """
        Initializes a process client
        @param proc a BaseProcess instance as originator of messages
        @param target  global scoped (process id or name) to send to
        @param targetname  system scoped exchange name to send messages to
        """
        if not proc:
            proc = BaseProcess()
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
