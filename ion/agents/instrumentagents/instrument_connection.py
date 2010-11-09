#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/instrument_connection.py
@author Dave Everett
@brief Instrument Connection Object
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor

from twisted.internet.protocol import Protocol

class InstrumentConnection(Protocol):
    """
    The InstrumentConnection class; inherits from Protocol.  Override
    connectionMade, connectionLost, and dataReceived and call parent methods
    for detailed handling.  
    """
    def __init__(self, parent):
        self.parent = parent

    def connectionMade(self):
        log.debug("connectionMade, calling gotConnected().")
        self.parent.gotConnected(self)

    def connectionLost(self, reason):
        log.debug("connectionLost, calling gotDisconnected()")
        self.parent.gotDisconnected(self)

    def dataReceived(self, data):
        """
        Filter the data; the instrument will send the
        prompt, which we don't care about, I'm assuming.  We might need a sort
        of state machine or something; for instance, the agent sends a getStatus
        command, we need to know that we're expecting a status message.
        """
        log.debug("dataReceived! Length: %s, data-[%s]" %(len(data), data))
        self.parent.gotData(data)


