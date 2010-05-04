#!/usr/bin/env python

"""
@file ion/agents/instrument_agents/SBE49_instrument_agent.py
@author Steve Foley
@brief CI interface for SeaBird SBE-49 CTD
"""
import logging
from twisted.internet import defer

from magnet.spawnable import Receiver
#from magnet.spawnable import send

#from ion.agents.resource_agent import lifecycleStates
#from ion.agents.resource_agent import ResourceAgent

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+ __name__)

from ion.agents.instrument_agents.instrument_agent import InstrumentAgent
from ion.core.base_process import ProtocolFactory

class SBE49InstrumentAgent(InstrumentAgent):

    """
    Maybe some day these values are looked up from a registry of common
        controlled vocabulary
    """
    __instrumentParameters = {    
        "baudrate": 9600,
        "outputformat": 0,
        "outputsal": "Y",
        "outputsv": "Y",
        "navg": 0,
        "mincondfreq": 0,
        "pumpdelay": 0,
        "tadvance": 0.0625,
        "alpha": 0.03,
        "tau": 7.0,
        "autorun": "Y",
        "tcaldate": "1/1/01",
        "ta0": 0.0,
        "ta1": 0.0,
        "ta2": 0.0,
        "ta3": 0.0,
        "toffset": 0.0,
        "ccaldate": "1/1/01",
        "cg": 0.0,
        "ch": 0.0,
        "ci": 0.0,
        "cj": 0.0,
        "cpcor": 0.0,
        "ctcor": 0.0,
        "cslope": 0.0,
        "pcaldate": "1/1/01",
        "prange": 100.0,
        "poffset": 0.0,
        "pa0": 0.0,
        "pa1": 0.0,
        "pa2": 0.0,
        "ptempa0": 0.0,
        "ptempa1": 0.0,
        "ptempa2": 0.0,
        "ptca0": 0.0,
        "ptca1": 0.0,
        "ptca2": 0.0,
        "ptcb0": 0.0,
        "ptcb1": 0.0,
        "ptcb2": 0.0
    }
    __instrumentCommands = (
        "setdefaults",
        "start",
        "stop",
        "pumpon",
        "pumpoff",
        "getsample",
        "test_temperature_converted",
        "test_conductivity_converted",
        "test_pressure_converted",
        "test_temperature_raw",
        "test_conductivity_raw",
        "test_pressure_raw"
        )

    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        React to a request for parameter values,
        @return A reply message containing a dictionary of name/value pairs
        """
        logging.debug('In op_get with received content: ' + str(content))
        response = {}
        for key in content:
            response[key] = self.__instrumentParameters.get(key)
        yield self.reply_message(msg, 'get', response, {})
    
    @defer.inlineCallbacks    
    def op_set(self, content, headers, msg):
        """
        Set parameters to the requested values.
        @return Message with a list of settings
        that were changed and what their new values are upon success.
        """
        logging.debug('In op_set with content: ' + str(content))
        for key in content:
            self.__instrumentParameters[key] = content[key]
        # Exception will bubble up if there is one, otherwise report success
        yield self.reply_message(msg, 'set', content, {})
    
    @defer.inlineCallbacks
    def op_getLifecycleState(self, content, headers, msg):
        """
        Query the lifecycle state of the object
        @return Message with the lifecycle state
        """
        yield self.reply_message(msg, 'getLifecycleState',
                                 self.lifecycleState, {})
    
    @defer.inlineCallbacks
    def op_setLifecycleState(self, content, headers, msg):
        """
        Set the lifecycle state
        @return Message with the lifecycle state that was set
        """
        self.lifecycleState = content
        yield self.reply_message(msg, 'setLifecycleState', content, {})
    
    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a specific command on the instrument, reply with a confirmation
        message including output of command, or simple ACK that command
        was executed.
        """
    
    @defer.inlineCallbacks
    def op_getStatus(self, content, headers, msg):
        """
        Obtain the status of an instrument. This includes non-parameter
        and non-lifecycle state of the instrument.
        """
    
    @defer.inlineCallbacks
    def op_getCapabilities(self, content, headers, msg):
        """
        Obtain a list of capabilities that this instrument has. This is
        simply a command and parameter list at this point
        """
        return_content = {'commands': __instrumentCommands,
                          'parameters': __instrumentParameters}
    
# Spawn of the process using the module name
factory = ProtocolFactory(SBE49InstrumentAgent)

"""
Someday the driver may inherit from a common (RS-232?) object if there is a need...
"""

class SBE49InstrumentDriver():
    def fetch_param(param):
        """
        operate in instrument protocol to get parameter
        """
        
    def set_param(param, value):
        """
        operate in instrument protocol to set a parameter
        """
        
    def execute(command):
        """
        Execute the given command
        """