#!/usr/bin/env python

"""
@file ion/core/base_process.py
@author Michael Meisinger
@author Stephen Pasco
@brief base class for all processes within Magnet
"""

import logging

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
    mechanics for developing processes, such as sending and receiving messages.

    @todo tighter integration with Spawnable
    """
    # Conversation ID counter
    convIdCnt = 0

    def __init__(self, receiver=None, spawnArgs=None):
        """
        Initialize process using an optional receiver and optional spawn args
        @param receiver  instance of a Receiver for process control
        @param spawnArgs  standard and additional spawn arguments
        """
        #logging.debug('BaseProcess.__init__()')
        self.procState = "UNINITIALIZED"
        spawnArgs = spawnArgs.copy() if spawnArgs else {}
        self.spawnArgs = spawnArgs
        self.init_time = pu.currenttime_ms()

        # Name (human readable label) of this process.
        self.procName = self.spawnArgs.get('proc-name', __name__)

        # The system unique ID; propagates from root supv to all child procs
        sysname = ioninit.cont_args.get('sysname', Container.id)
        self.sysName = self.spawnArgs.get('sys-name', sysname)

        # The process ID of the supervisor process
        self.procSupId = pu.get_process_id(self.spawnArgs.get('sup-id', None))

        if not receiver:
            receiver = Receiver(self.procName)
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
                self.procName, self.procSupId, self.sysName))

    def add_receiver(self, receiver):
        key = receiver.name
        self.receivers[key] = receiver

    @defer.inlineCallbacks
    def spawn(self):
        """
        Spawns this process using the process' receiver. Self spawn can
        only be called once per instance.
        @note this method is not called when spawned by magnet
        """
        assert not self.receiver.spawned, "Process already spawned"
        self.id = yield spawn(self.receiver)
        logging.debug('spawn()=' + str(self.id))
        defer.returnValue(self.id)

    def is_spawned(self):
        return self.receiver.spawned != None

    @defer.inlineCallbacks
    def op_init(self, content, headers, msg):
        """
        Init operation, on receive of the init message
        """
        if self.procState == "UNINITIALIZED":
            yield defer.maybeDeferred(self.plc_init)
            logging.info('----- Process %s INITIALIZED -----' % (self.procName))

            yield self.reply_ok(msg)
            self.procState = "INITIALIZED"

    def plc_init(self):
        """
        Process life cycle event: on initialization of process (once)
        """
        logging.info('BaseProcess.plc_init()')

    def receive(self, payload, msg):
        """
        This is the main entry point for received messages. Messages are
        dispatched to operation handling methods. RPC is caught and completed.
        """
        # Check if this response is in reply to an RPC call
        if 'conv-id' in payload and payload['conv-id'] in self.rpc_conv:
            logging.info('BaseProcess: Received RPC reply. content=' +str(payload['content']))
            d = self.rpc_conv.pop(payload['conv-id'])
            content = payload.get('content', None)
            res = (content, payload, msg)
            # @todo is it OK to ack the response at this point already?
            d1 = msg.ack()
            if d1:
                # Support for older carrot version where ack did not return
                d1.addCallback(lambda res1: d.callback(res))
                d1.addErrback(lambda c: d.errback(c))
        else:
            logging.info('BaseProcess: Message received, dispatching...')
            convid = payload.get('conv-id', None)
            conv = self.conversations.get(convid, None) if convid else None
            # Perform a dispatch of message by operation
            d = pu.dispatch_message(payload, msg, self, conv)
            def _cb(res):
                logging.info("ACK msg")
                d1 = msg.ack()
            d.addCallbacks(_cb, logging.error)

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
            scoped_name =  self.sysName + "." + name
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
        yield procRegistry.put(str(childproc.procName), str(child_id))
        if init:
            yield childproc.init()
        defer.returnValue(child_id)

    def link_child(self, supervisor):
        pass

    def spawn_link(self, childproc, supervisor):
        pass

    def get_child_def(self, name):
        """
        @retval the ProcessDesc instance of a child process by name
        """
        for child in self.child_procs:
            if child.procName == name:
                return child

    def get_child_id(self, name):
        """
        @retval the process id a child process by name
        """
        child = self.get_child_def(name)
        return child.procId if child else None

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
        self.procName = kwargs.get('name', None)
        self.procModule = kwargs.get('module', None)
        self.procClass = kwargs.get('class', kwargs.get('procclass', None))
        self.procNode = kwargs.get('node', None)
        self.spawnArgs = kwargs.get('spawnargs', None)
        self.procId = None
        self.procState = 'DEFINED'

    @defer.inlineCallbacks
    def spawn(self, supProc=None):
        """
        Spawns this process description with the initialized attributes.
        @param supProc  the process instance that should be set as supervisor
        """
        assert self.procState == 'DEFINED', "Cannot spawn process twice"
        self.supProcess = supProc
        if self.procNode == None:
            logging.info('Spawning name=%s node=%s' % (self.procName, self.procNode))

            # Importing service module
            proc_mod = pu.get_module(self.procModule)
            self.procModObj = proc_mod

            # Spawn instance of a process
            # During spawn, the supervisor process id, system name and proc name
            # get provided as spawn args, in addition to any give spawn args.
            spawnargs = {'proc-name':self.procName,
                         'sup-id':self.supProcess.receiver.spawned.id.full,
                         'sys-name':self.supProcess.sysName}
            if self.spawnArgs:
                spawnargs.update(self.spawnArgs)
            #logging.debug("spawn(%s, args=%s)" % (self.procModule, spawnargs))
            proc_id = yield spawn(proc_mod, None, spawnargs)
            self.procId = proc_id
            self.procState = 'SPAWNED'

            #logging.info("Process "+self.procClass+" ID: "+str(proc_id))
        else:
            logging.error('Cannot spawn '+self.procClass+' on node='+str(self.procNode))
        defer.returnValue(self.procId)

    @defer.inlineCallbacks
    def init(self):
        (content, headers, msg) = yield self.supProcess.rpc_send(self.procId,
                                                'init', {}, {'quiet':True})
        if content.get('status','ERROR') == 'OK':
            self.procState = 'INIT_OK'
        else:
            self.procState = 'INIT_ERROR'


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
        self._check_init()

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
