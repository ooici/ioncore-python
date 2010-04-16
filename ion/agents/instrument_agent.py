#!/usr/bin/env python

from ion.agents.resource_agent import ResourceAgent

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

store = Store()

receiver = Receiver(__name__)

@defer.inlineCallbacks
def start():
    id = yield spawn(receiver)
    store.put('instrument_agent', id)

class InstrumentAgent(ResourceAgent):
    
    def op_get(self, param_name):
        """
        """
        
    def op_set(self, param_name, value):
        """
        """
    
    def op_getLifecycleState(self):
        """
        """
    
    def op_setLifecycleState(self, state_value):
        """
        """
    
    def op_execute(self, command):
        """
        """
    
    def op_getStatus(self):
        """
        """
    
    def op_getCapabilities(self):
        """
        """
        
def receive(content, msg):
    instance.receive(content,msg)

instance = InstrumentAgent()

receiver.handle(receive)