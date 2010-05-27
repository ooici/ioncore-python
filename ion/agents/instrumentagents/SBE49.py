#!/usr/bin/env python

"""
@file ion/agents/instrument_agents/SBE49_instrument_agent.py
@author Steve Foley
@brief CI interface for SeaBird SBE-49 CTD
"""
import logging
from twisted.internet import defer

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+ __name__)

from ion.agents.instrumentagents.instrument_agent import InstrumentAgent
from ion.core.base_process import ProtocolFactory
from ion.services.coi.resource_registry import ResourceLCState as LCS


instrumentCommands = (
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

"""
Maybe some day these values are looked up from a registry of common
controlled vocabulary
"""
instrumentParameters = (    
    "baudrate",
    "outputformat",
    "outputsal",
    "outputsv",
    "navg",
    "mincondfreq",
    "pumpdelay",
    "tadvance",
    "alpha",
    "tau",
    "autorun",
    "tcaldate",
    "ta0",
    "ta1",
    "ta2",
    "ta3",
    "toffset",
    "ccaldate",
    "cg",
    "ch",
    "ci",
    "cj",
    "cpcor",
    "ctcor",
    "cslope",
    "pcaldate",
    "prange",
    "poffset",
    "pa0",
    "pa1",
    "pa2",
    "ptempa0",
    "ptempa1",
    "ptempa2",
    "ptca0",
    "ptca1",
    "ptca2",
    "ptcb0",
    "ptcb1",
    "ptcb2"
)

"""
Someday the driver may inherit from a common (RS-232?) object if there is a need...
"""

class SBE49InstrumentDriver():
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
    
    def fetch_param(self, param):
        """
        operate in instrument protocol to get parameter
        """
        return self.__instrumentParameters[param]
    
    def set_param(self, param, value):
        """
        operate in instrument protocol to set a parameter
        """
        if (param in self.__instrumentParameters):
            self.__instrumentParameters[param] = value
            return {param: value}
        else:
            return {}
            
    def execute(self, command):
        """
        Execute the given command
        """
        if command in instrumentCommands:
            return (1, command)
        else:
            return (0, command)
        
class SBE49InstrumentAgent(InstrumentAgent):

    __driver = SBE49InstrumentDriver()
    lifecycleState = LCS.RESLCS_NEW
    
    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        React to a request for parameter values,
        @return A reply message containing a dictionary of name/value pairs
        """
        assert(isinstance(content, list))
        response = {}
        for key in content:
            response[key] = self.__driver.fetch_param(key)
        if response != {}:
            yield self.reply_ok(msg, response)
        else:
            yield self.reply_err(msg, 'No values found')
    
    @defer.inlineCallbacks    
    def op_set(self, content, headers, msg):
        """
        Set parameters to the requested values.
        @return Message with a list of settings
            that were changed and what their new values are upon success.
            On failure, return the bad key, but previous keys were already set
        """
        assert(isinstance(content, dict))
        response = {}
        for key in content:
            result = {}
            result = self.__driver.set_param(key, content[key])
            if result == {}:
                yield self.reply_err(msg, "Could not set %s" % key)
            else:
                response.update(result)
        assert(response != {})
        yield self.reply_ok(msg, response)
            
    @defer.inlineCallbacks
    def op_getLifecycleState(self, content, headers, msg):
        """
        Query the lifecycle state of the object
        @return Message with the lifecycle state
        """
        yield self.reply(msg, 'getLifecycleState', self.lifecycleState, {})
        
    @defer.inlineCallbacks
    def op_setLifecycleState(self, content, headers, msg):
        """
        Set the lifecycle state
        @return Message with the lifecycle state that was set
        """
        self.lifecycleState = content
        yield self.reply(msg, 'setLifecycleState', content, {})
    
    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a specific command on the instrument, reply with a confirmation
        message including output of command, or simple ACK that command
        was executed.
        @param content Should be a list where first element is the command,
            the rest are the arguments
        """
        assert(isinstance(content, unicode))
        execResult = self.__driver.execute(content)
        assert(len(execResult) == 2)
        (errorCode, response) = execResult
        assert(isinstance(errorCode, int))
        if errorCode == 1:
            yield self.reply_ok(msg, response)
        else:
            yield self.reply_err(msg,
                                 "Error code %s, response: %s" % (errorCode,
                                                                  response))
    
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
        yield self.reply(msg, 'getCapabilties',
                         {'commands': instrumentCommands,
                          'parameters': instrumentParameters}, {})

# Spawn of the process using the module name
factory = ProtocolFactory(SBE49InstrumentAgent)
