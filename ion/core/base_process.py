#!/usr/bin/env python

"""
@file ion/core/base_process.py
@author Michael Meisinger
@author Stephen Pasco
@brief base class for all processes within Magnet
"""

import logging

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

import ion.util.procutils as pu

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class BaseProcess(object):
    """
    This is the base class for all processes. Processes are Spawnables before
    and after they are spawned.
    @todo tighter integration with Spawnable
    """

    convIdCnt = 0
    
    def __init__(self, receiver=Receiver(__name__)):
        """Constructor using a given name for the spawnable receiver.
        """
        logging.debug('BaseProcess.__init__()')
        
        self.procName = __name__
        self.idStore = Store()
        self.receiver = receiver
        receiver.handle(self.receive)

    def receive(self, content, msg):
        logging.info('BaseProcess.receive()')
        self.dispatch_message(content, msg)

    def dispatch_message(self, content, msg):
        pu.dispatch_message(content, msg, self)
        
    def op_noop_catch(self, content, headers, msg):
        """The method called if operation is not defined
        """
        logging.info('Catch message')

    def send_message(self, recv, operation, content, headers):
        """Send a message via the process receiver to destination. Starts a new conversation.
        """
        send = self.receiver.spawned.id.full
        BaseProcess.convIdCnt += 1
        convid = "#" + str(BaseProcess.convIdCnt)
        #convid = send + "#" + BaseProcess.convIdCnt
        msgheaders = {}
        msgheaders.update(headers)
        msgheaders['conv-id'] = convid
        pu.send_message(self.receiver, send, recv, operation, content, msgheaders)

    def reply_message(self, msg, operation, content, headers):
        ionMsg = msg.payload
        send = self.receiver.spawned.id.full
        recv = ionMsg.get('reply-to', None)
        if recv == None:
            log.error('No reply-to given for message '+str(msg))
        else:
            headers['conv-id'] = ionMsg.get('conv-id','')
            self.send_message(pu.get_process_id(recv), operation, content, headers)
