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
from magnet.store import Store

from ion.core import ioninit
import ion.util.procutils as pu

CONF = ioninit.config(__name__)
CF_conversation_log = CONF['conversation_log']
CF_container_group = ioninit.ion_config.getValue2('ion.core.bootstrap','container_group',Container.id)

# Static store (kvs) to register process instances with names
procRegistry = Store()

class BaseProcess(object):
    """
    This is the base class for all processes. Processes are Spawnables before
    and after they are spawned.
    @todo tighter integration with Spawnable
    """

    convIdCnt = 0

    def __init__(self, receiver=None, spawnArgs=None):
        """Constructor using a given name for the spawnable receiver.
        """
        logging.debug('BaseProcess.__init__()')
        self.procState = "UNINITIALIZED"
        if not receiver:
            receiver = Receiver(__name__)
        spawnArgs = spawnArgs.copy() if spawnArgs else {}

        self.procName = __name__
        self.sysName = Container.id # The ID that originates from the root supv
        self.idStore = Store()
        self.receiver = receiver
        self.spawnArgs = spawnArgs
        receiver.handle(self.receive)

    @defer.inlineCallbacks
    def op_init(self, content, headers, msg):
        """Init operation, on receive of the init message
        """
        logging.info('BaseProcess.op_init: '+str(content))
        if self.procState == "UNINITIALIZED":
            self.sysName = content.get('sys-name', Container.id)
            self.procName = content.get('proc-name', __name__)
            supId = content.get('sup-id', None)
            self.procSupId = pu.get_process_id(supId)
            logging.info('BaseProcess.op_init: proc-name='+self.procName+', sup-id='+supId)

            r = yield defer.maybeDeferred(self.plc_init)
            logging.info('===== Process '+self.procName+' INITIALIZED ============')

            self.reply_message(msg, 'inform_init', {'status':'OK'}, {})

            self.procState = "INITIALIZED"

    def plc_init(self):
        """Process life cycle event: on initialization of process (once)
        """
        logging.info('BaseProcess.plc_init()')

    def receive(self, content, msg):
        logging.info('BaseProcess.receive()')
        d = self.dispatch_message(content, msg)
        def _cb(res):
            logging.info("******ACK msg")
            msg.ack()
        d.addCallback(_cb)
        d.addErrback(logging.error)
        
    def dispatch_message(self, content, msg):
        return pu.dispatch_message(content, msg, self)

    def op_noop_catch(self, content, headers, msg):
        """The method called if operation is not defined
        """
        logging.info('Catch message')

    @defer.inlineCallbacks
    def send_message(self, recv, operation, content, headers):
        """Send a message via the process receiver to destination.
        Starts a new conversation.
        """
        send = self.receiver.spawned.id.full
        BaseProcess.convIdCnt += 1
        convid = "#" + str(BaseProcess.convIdCnt)
        #convid = send + "#" + BaseProcess.convIdCnt
        msgheaders = {}
        msgheaders.update(headers)
        msgheaders['conv-id'] = convid
        yield pu.send_message(self.receiver, send, recv, operation, content, msgheaders)
        self.log_conv_message()

    def reply_message(self, msg, operation, content, headers):
        """Replies to a given message, continuing the ongoing conversation
        """
        ionMsg = msg.payload
        send = self.receiver.spawned.id.full
        recv = ionMsg.get('reply-to', None)
        if recv == None:
            logging.error('No reply-to given for message '+str(msg))
        else:
            headers['conv-id'] = ionMsg.get('conv-id','')
            self.send_message(pu.get_process_id(recv), operation, content, headers)
            self.log_conv_message()

    def log_conv_message(self):
        pass
        #if CF_conversation_log:
        #    send = self.receiver.spawned.id.full
            #pu.send_message(self.receiver, send, '', 'logmsg', {}, {})

    def get_local_name(self, name):
        """Returns a name that is qualified by the system name. System name is
        the ID of the container the originated the entire system"""
        return self.sysName + "." + name

    def get_group_name(self, name):
        """Returns a name that is qualified by a configured group name."""
        return CF_container_group + "." + name

    def get_scoped_name(self, scope, name):
        """Returns a name that is scoped.
        @param scope  one of "local", "group" or "global"
        @param name name to be scoped
        """
        if scope == 'local': return self.get_local_name(name)
        if scope == 'group': return self.get_group_name(name)
        return  name

class ProtocolFactory(ProtocolFactory):
    """Standard protocol factory that is instantiated in each service process
    module. Returns a new receiver for each invocation and creates a new
    process instance.
    """

    def __init__(self, processClass, name=__name__, args={}):
        self.processClass = processClass
        self.name = name
        self.args = args

    def build(self, spawnArgs={}):
        """Factory method return a new receiver for a new process. At the same
        time instantiate class.
        """
        logging.info("ProtocolFactory.build() of name="+self.name+" with args="+str(spawnArgs))
        receiver = self.receiver(self.name)
        instance = self.processClass(receiver, spawnArgs)
        receiver.procinst = instance
        return receiver
    
class RpcClient(object):
    """Service client providing a RPC methaphor
    """

    def __init__(self):
        self.clientRecv = Receiver(__name__)
        self.clientRecv.handle(self.receive)
        self.deferred = None

    @defer.inlineCallbacks
    def attach(self):
        self.id = yield spawn(self.clientRecv)

    def rpc_send(self, to, op, cont='', headers={}):
        """
        @retval a deferred with the message value
        """
        d = pu.send_message(self.clientRecv, self.id, to, op, cont, headers)
        # Ignore d deferred, wait for send. TODO: error handling
        self.deferred = defer.Deferred()
        return self.deferred

    def receive(self, headers, msg):
        pu.log_message(__name__, headers, msg)
        logging.info('RpcClient.receive(), calling callback in defer')
        msg.ack()
        content = headers.get('content',None)
        res = (content, headers, msg)
        self.deferred.callback(res)
