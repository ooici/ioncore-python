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

CONF = ioninit.config(__name__)
CF_conversation_log = CONF['conversation_log']

# @todo CHANGE: Static store (kvs) to register process instances with names
procRegistry = Store()


# @todo HACK: List of process instances
receivers = []

class BaseProcess(object):
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
        self.proc_state = "NEW"
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
        self.receiver = ProcessReceiver(label=self.proc_name,
                                        name=str(self.id),
                                        group=self.proc_group,
                                        process=self)
        self.receiver.add_handler(self.receive)

        # Create a backend receiver for outgoing RPC process interactions.
        # Needed to avoid deadlock when processing incoming messages
        # because only one message can be consumed before ACK.
        self.backend_id = ProcessInstantiator.create_process_id()
        self.backend_receiver = ProcessReceiver(label=self.proc_name + "_back",
                                         name=str(self.backend_id),
                                         group=self.proc_group,
                                         process=self)
        self.backend_receiver.add_handler(self.receive)

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

        log.debug("NEW Process [%s], sup-id=%s, sys-name=%s, backend=%s" % (
                self.proc_name, self.proc_supid, self.sys_name, self.backend_id))

    def add_receiver(self, receiver):
        self.receivers[receiver.name] = receiver

    @defer.inlineCallbacks
    def spawn(self):
        """
        Spawns this process using the process' receiver and initializes it in
        the same call. Self spawn can only be called once per instance.
        """
        assert not self.receiver.consumer, "Process already spawned"
        assert not self.backend_receiver.consumer, "Process already spawned"

        for rec in self.receivers.values():
            print "rec", rec
            yield rec.activate()

        log.debug('Process spawn(): pid=%s' % (self.id))
        yield defer.maybeDeferred(self.plc_spawn)

        # Call init right away. This is what you would expect anyways in a
        # container executed spawn
        #yield self.init()

        defer.returnValue(self.id)

    def init(self):
        """
        DO NOT CALL. Automatically called by spawn().
        Initializes this process instance. Typically a call should not be
        necessary because the init message is received from the supervisor
        process. It may be necessary for the root supervisor and for test
        processes.
        @retval Deferred
        """
        if self.proc_state == "NEW":
            return self.op_init(None, None, None)
        else:
            return defer.succeed(None)

    def plc_spawn(self):
        """
        Process life cycle event: on spawn of process (once)
        """

    def is_spawned(self):
        return self.receiver.consumer != None

    @defer.inlineCallbacks
    def op_init(self, content, headers, msg):
        """
        Init operation, on receive of the init message
        """
        if self.proc_state == "NEW":
            # @todo: Right after giving control to the process specific init,
            # the process can enable message consumption and messages can be
            # received. How to deal with the situation that the process is not
            # fully initialized yet???? Stop message floodgate until init'd?

            # Change state from NEW early, to prevent consistenct probs.
            self.proc_state = "INIT"

            try:
                yield defer.maybeDeferred(self.plc_init)
                self.proc_state = "ACTIVE"
                log.info('----- Process %s INIT OK -----' % (self.proc_name))
                if msg != None:
                    # msg is None only if called from local process self.init()
                    yield self.reply_ok(msg)
            except Exception, ex:
                self.proc_state = "ERROR"
                log.exception('----- Process %s INIT ERROR -----' % (self.proc_name))
                if msg != None:
                    # msg is None only if called from local process self.init()
                    yield self.reply_err(msg, "Process %s INIT ERROR" % (self.proc_name) + str(ex))
        else:
            self.proc_state = "ERROR"
            log.error('Process %s in wrong state %s for op_init' % (self.proc_name, self.proc_state))

    def plc_init(self):
        """
        Process life cycle event: on initialization of process (once)
        """

    @defer.inlineCallbacks
    def op_shutdown(self, content, headers, msg):
        """
        Init operation, on receive of the init message
        """
        assert self.proc_state == "ACTIVE", "Process not initalized"

        if len(self.child_procs) > 0:
            log.info("Shutting down child processes")
        while len(self.child_procs) > 0:
            child = self.child_procs.pop()
            res = yield self.shutdown_child(child)

        yield defer.maybeDeferred(self.plc_shutdown)
        log.info('----- Process %s TERMINATED -----' % (self.proc_name))

        if msg != None:
                # msg is None only if called from local process self.shutdown()
            yield self.reply_ok(msg)
        self.proc_state = "TERMINATED"

    def plc_shutdown(self):
        """
        Process life cycle event: on shutdown of process (once)
        """

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
                d = self.reply_err(msg, 'ERROR in process receive(): '+str(ex))

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
        @retval deferred
        """
        #@BUG Added hack to handle messages from plc_init in cc_agent!
        if payload['op'] == 'init' or \
                self.proc_state == "INIT" or self.proc_state == "ACTIVE" or \
                (payload['op'] == 'identify' and payload['content']=='started'):
            # Regular message handling in expected state
            if payload['op'] != 'init' and self.proc_state == "INIT":
                log.warn('Process %s received message before completed init' % (self.proc_name))

            d = pu.dispatch_message(payload, msg, target, conv)
            return d
        else:
            text = "Process %s in invalid state %s." % (self.proc_name, self.proc_state)
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

    def send(self, recv, operation, content, headers=None):
        """
        @brief Send a message via the process receiver to destination.
        Starts a new conversation.
        @retval Deferred for send of message
        """
        send = self.backend_id
        msgheaders = self._prepare_message(headers)
        return pu.send(self.backend_receiver, send, recv, operation, content, msgheaders)

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
            return self.send(pu.get_process_id(recv), operation, content, headers)

    def reply_ok(self, msg, content=None, headers=None):
        """
        Glue method that replies to a given message with a success message and
        a given result value
        @retval Deferred for send of reply
        """
        rescont = {'status':'OK'}
        if type(content) is dict:
            rescont.update(content)
        else:
            rescont['value'] = content
        return self.reply(msg, 'result', rescont, headers)

    def reply_err(self, msg, content=None, headers=None):
        """
        Glue method that replies to a given message with an error message and
        an indication of the error.
        @retval Deferred for send of reply
        """
        rescont = {'status':'ERROR'}
        if type(content) is dict:
            rescont.update(content)
        else:
            rescont['value'] = content
        return self.reply(msg, 'result', rescont, headers)

    def get_conversation(self, headers):
        convid = headers.get('conv-id', None)
        return self.conversations(convid, None)

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
    def spawn_child(self, childproc, init=True):
        """
        Spawns a process described by the ProcessDesc instance as child of this
        process instance. An init message is sent depending on flag.
        @param childproc  ProcessDesc instance with attributes
        @param init  flag determining whether an init message should be sent
        @retval process id of the child process
        """
        assert isinstance(childproc, ProcessDesc)
        assert not childproc in self.child_procs
        self.child_procs.append(childproc)
        child_id = yield childproc.spawn(self)
        yield procRegistry.put(str(childproc.proc_name), str(child_id))
        if init:
            yield childproc.init()
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

    def shutdown(self):
        """
        Recursivey terminates all child processes and then itself.
        @retval Deferred
        """
        return self.op_shutdown(None, None, None)

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
