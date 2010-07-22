#!/usr/bin/env python

"""
@file ion/core/base_process.py
@author Michael Meisinger
@author Stephen Pasco
@brief base class for all processes within Magnet
"""

import logging
logging = logging.getLogger(__name__)

from twisted.internet import defer
from magnet.container import Container
from magnet.spawnable import Receiver
from magnet.spawnable import ProtocolFactory
from magnet.spawnable import spawn
from ion.data.store import Store

from ion.core import ioninit
from ion.interact.conversation import Conversation
from ion.interact.message import Message
import ion.util.procutils as pu

CONF = ioninit.config(__name__)
CF_conversation_log = CONF['conversation_log']

# Define the exported public names of this module
__all__ = ['BaseProcess','ProcessDesc','ProtocolFactory','Message','processes','procRegistry']

# Static store (kvs) to register process instances with names
# @todo CHANGE
procRegistry = Store()

# @todo HACK: Dict of process "alias" to process declaration
processes = {}

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
    # Conversation ID counter
    convIdCnt = 0

    def __init__(self, receiver=None, spawnArgs=None, **kwargs):
        """
        Initialize process using an optional receiver and optional spawn args
        @param receiver  instance of a Receiver for process control
        @param spawnArgs  standard and additional spawn arguments
        """
        self.proc_state = "UNINITIALIZED"
        spawnArgs = spawnArgs.copy() if spawnArgs else {}
        self.spawn_args = spawnArgs
        self.proc_init_time = pu.currenttime_ms()

        # Name (human readable label) of this process.
        self.proc_name = self.spawn_args.get('proc-name', __name__)

        # The system unique ID; propagates from root supv to all child procs
        sysname = ioninit.cont_args.get('sysname', Container.id)
        self.sys_name = self.spawn_args.get('sys-name', sysname)

        # The process ID of the supervisor process
        self.proc_supid = pu.get_process_id(self.spawn_args.get('sup-id', None))

        if not receiver:
            receiver = Receiver(self.proc_name)
        self.receiver = receiver
        receiver.handle(self.receive)

        # Dict of all receivers of this process. Key is the name
        self.receivers = {}
        self.add_receiver(self.receiver)

        # Dict of converations.
        # @todo: make sure this is garbage collected once in a while
        self.conversations = {}
        # Conversations by conv-id for currently outstanding RPCs
        self.rpc_conv = {}

        # List of ProcessDesc instances of defined and spawned child processes
        self.child_procs = []

        logging.info("Process init'd: proc-name=%s, sup-id=%s, sys-name=%s" % (
                self.proc_name, self.proc_supid, self.sys_name))

    def add_receiver(self, receiver):
        key = receiver.name
        self.receivers[key] = receiver

    @defer.inlineCallbacks
    def spawn(self):
        """
        Spawns this process using the process' receiver. Self spawn can
        only be called once per instance.
        @note this method is not called when spawned through magnet. This makes
        it tricky to do consistent initialization on spawn.
        """
        assert not self.receiver.spawned, "Process already spawned"
        self.id = yield spawn(self.receiver)
        logging.debug('spawn()=' + str(self.id))
        yield defer.maybeDeferred(self.plc_spawn)
        defer.returnValue(self.id)

    def plc_spawn(self):
        """
        Process life cycle event: on spawn of process (once)
        """

    def is_spawned(self):
        return self.receiver.spawned != None

    @defer.inlineCallbacks
    def op_init(self, content, headers, msg):
        """
        Init operation, on receive of the init message
        """
        if self.proc_state == "UNINITIALIZED":
            yield defer.maybeDeferred(self.plc_init)
            logging.info('----- Process %s INITIALIZED -----' % (self.proc_name))

            if msg != None:
                # msg is None only if called from local process self.init()
                yield self.reply_ok(msg)
            self.proc_state = "INITIALIZED"

    def plc_init(self):
        """
        Process life cycle event: on initialization of process (once)
        """
        logging.info('BaseProcess.plc_init()')

    @defer.inlineCallbacks
    def op_shutdown(self, content, headers, msg):
        """
        Init operation, on receive of the init message
        """
        assert self.proc_state == "INITIALIZED", "Process must be initalized"

        if len(self.child_procs) > 0:
            logging.info("Shutting down child processes")
        while len(self.child_procs) > 0:
            child = self.child_procs.pop()
            res = yield self.shutdown_child(child)

        yield defer.maybeDeferred(self.plc_shutdown)
        logging.info('----- Process %s TERMINATED -----' % (self.proc_name))

        if msg != None:
                # msg is None only if called from local process self.shutdown()
            yield self.reply_ok(msg)
        self.proc_state = "TERMINATED"

    def plc_shutdown(self):
        """
        Process life cycle event: on shutdown of process (once)
        """
        logging.info('BaseProcess.plc_shutdown()')

    def receive(self, payload, msg):
        """
        This is the main entry point for received messages. Messages are
        distinguished into RPC replies (by conversation ID) and other received
        messages.
        """
        # Check if this response is in reply to an RPC call
        if 'conv-id' in payload and payload['conv-id'] in self.rpc_conv:
            self._receive_rpc(payload, msg)
        else:
            self._receive_msg(payload, msg)

    def _receive_rpc(self, payload, msg):
        """
        Handling of RPC replies.
        """
        logging.info('BaseProcess: Received RPC reply.')
        d = self.rpc_conv.pop(payload['conv-id'])
        content = payload.get('content', None)
        res = (content, payload, msg)
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
        logging.info('BaseProcess: Message received, dispatching...')
        convid = payload.get('conv-id', None)
        conv = self.conversations.get(convid, None) if convid else None
        # Perform a dispatch of message by operation
        d = self._dispatch_message(payload, msg, self, conv)
        def _cb(res):
            logging.info("ACK msg")
            d1 = msg.ack()
        d.addCallbacks(_cb, logging.error)
        return d

    def _dispatch_message(self, payload, msg, target, conv):
        """
        Dispatch of messages to operations within this process instance. The
        default behavior is to dispatch to 'op_*' functions, where * is the
        'op' message attribute.
        @retval deferred
        """
        #@BUG Added hack to handle messages from plc_init in cc_agent!
        #assert payload['op'] == 'init' or self.proc_state == "INITIALIZED"
        assert payload['op'] == 'init' or self.proc_state == "INITIALIZED" or (payload['op'] == 'identify' and payload['content']=='started')
        d = pu.dispatch_message(payload, msg, target, conv)
        return d

    def op_none(self, content, headers, msg):
        """
        The method called if operation callback operation is not defined
        """
        logging.info('Catch message')

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
            logging.info("RPC on conversation %s timed out! "%(convid))
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
        send = self.receiver.spawned.id.full
        msgheaders = self._prepare_message(headers)
        return pu.send(self.receiver, send, recv, operation, content, msgheaders)

    def _prepare_message(self, headers):
        msgheaders = {}
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
        send = self.receiver.spawned.id.full
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
            logging.error('No reply-to given for message '+str(msg))
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
            scoped_name =  str(Container.id) + "." + name
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

    def init(self):
        """
        Initializes this process instance. Typically a call should not be
        necessary because the init message is received from the supervisor
        process. It may be necessary for the root supervisor and for test
        processes.
        @retval Deferred
        """
        return self.op_init(None, None, None)

    def shutdown(self):
        """
        Recursivey terminates all child processes and then itself.
        @retval Deferred
        """
        return self.op_shutdown(None, None, None)

class ProcessDesc(object):
    """
    Class that encapsulates attributes about a spawnable process; can spawn
    and init processes.
    """
    def __init__(self, **kwargs):
        """
        Initializes ProcessDesc instance with process attributes
        @param name  name label of process
        @param module  module name of process module
        @param class  or procclass is class name in process module (optional)
        @param node  ID of container to spawn process on (optional)
        @param spawnargs  dict of additional spawn arguments (optional)
        """
        self.proc_name = kwargs.get('name', None)
        self.proc_module = kwargs.get('module', None)
        self.proc_class = kwargs.get('class', kwargs.get('procclass', None))
        self.proc_node = kwargs.get('node', None)
        self.spawn_args = kwargs.get('spawnargs', None)
        self.proc_id = None
        self.proc_state = 'DEFINED'

    @defer.inlineCallbacks
    def spawn(self, supProc=None):
        """
        Spawns this process description with the initialized attributes.
        @param supProc  the process instance that should be set as supervisor
        """
        assert self.proc_state == 'DEFINED', "Cannot spawn process twice"
        self.sup_process = supProc
        if self.proc_node == None:
            logging.info('Spawning name=%s node=%s' %
                         (self.proc_name, self.proc_node))

            # Importing service module
            proc_mod = pu.get_module(self.proc_module)
            self.proc_mod_obj = proc_mod

            # Spawn instance of a process
            # During spawn, the supervisor process id, system name and proc name
            # get provided as spawn args, in addition to any give spawn args.
            spawnargs = {'proc-name':self.proc_name,
                         'sup-id':self.sup_process.receiver.spawned.id.full,
                         'sys-name':self.sup_process.sys_name}
            if self.spawn_args:
                spawnargs.update(self.spawn_args)
            #logging.debug("spawn(%s, args=%s)" % (self.proc_module, spawnargs))
            proc_id = yield spawn(proc_mod, None, spawnargs)
            self.proc_id = proc_id
            self.proc_state = 'SPAWNED'

            logging.info("Process "+self.proc_class+" ID: "+str(proc_id))
        else:
            logging.error('Cannot spawn '+self.proc_class+' on node='+str(self.proc_node))
        defer.returnValue(self.proc_id)

    @defer.inlineCallbacks
    def init(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'init', {}, {'quiet':True})
        if content.get('status','ERROR') == 'OK':
            self.proc_state = 'INIT_OK'
        else:
            self.proc_state = 'INIT_ERROR'

    @defer.inlineCallbacks
    def shutdown(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'shutdown', {}, {'quiet':True})
        if content.get('status','ERROR') == 'OK':
            self.proc_state = 'TERMINATED'
        else:
            self.proc_state = 'SHUTDOWN_ERROR'


class ProtocolFactory(ProtocolFactory):
    """
    This protocol factory returns receiver instances used to spawn processes
    from a module. This implementation creates process class instances together
    with the receiver. This is a standard implementation that can be used
    in the code of every module containing a process. This factory also collects
    process declarations alongside.
    """
    def __init__(self, pcls, name=None, args=None):
        self.processClass = pcls
        if not name:
            name = pcls.__name__
        self.name = name
        if not args:
            args = {}
        self.args = args
        # Collecting the declare static class variable in a process class
        if pcls and hasattr(pcls, 'declare') and type(pcls.declare) is dict:
            procdec = pcls.declare.copy()
            procdec['class'] = pcls
            procname = pcls.declare.get('name', pcls.__name__)
            if procname in processes:
                raise RuntimeError('Process already declared: '+str(procname))
            processes[procname] = procdec

    def build(self, spawnArgs=None):
        """
        Factory method return a new receiver for a new process. At the same
        time instantiate class.
        """
        if not spawnArgs:
            spawnArgs = {}
        #logging.debug("ProtocolFactory.build(name=%s, args=%s)" % (self.name,spawnArgs))
        receiver = self.receiver(spawnArgs.get('proc-name', self.name))
        receiver.group = self.name
        instance = self.processClass(receiver, spawnArgs)
        receiver.procinst = instance
        receivers.append(receiver)
        return receiver

# Spawn of the process using the module name
factory = ProtocolFactory(BaseProcess)

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
