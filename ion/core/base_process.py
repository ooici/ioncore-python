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

    idStore = None
    receiver = None
    
    def __init__(self, receiver=Receiver(__name__)):
        """Constructor using a given name for the spawnable receiver.
        """
        logging.debug('BaseProcess.__init__()')
        
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

    def send_message(self, to, operation, content, headers):
        """Send a message via the processes receiver to a
        """
        src = ""
        pu.send_message(self.receiver, src, to, operation, content, headers)
