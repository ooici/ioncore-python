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

    """ Called via ion.core.base_process.BaseProcess.dispatch_message """
    def op_get(self, content, headers, msg):
        print 'in get with received content: ', content
        response = {}
        for key in content:
            response[key] = self.__instrumentParameters.get(key)
        print 'responding with: ', response
        
    def op_set(self,content, headers, msg):
        print 'in set with content: ', content
        for key in content:
            print 'setting ', self.__instrumentParameters[key], ' to ', \
                content[key]
            self.__instrumentParameters[key] = content[key]
            
    
    def op_getLifecycleState(self, content, headers, msg):
        print 'lifecycleState is: ', self.lifecycleState
    
    def op_setLifecycleState(self, content, headers, msg):
        self.lifecycleState = content
    
    def op_execute(self, content, headers, msg):
        """
        """
    
    def op_getStatus(self, content, headers, msg):
        """
        """
    
    def op_getCapabilities(self, content, headers, msg):
        """
        """
    
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