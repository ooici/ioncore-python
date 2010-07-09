#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE49_instrument_agent.py
@author Steve Foley
@brief CI interface for SeaBird SBE-49 CTD
"""
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.agents.instrumentagents import instrument_agent as IA
from ion.agents.instrumentagents.instrument_agent import InstrumentAgent
from ion.agents.instrumentagents.instrument_agent import InstrumentDriver
from ion.core.base_process import ProtocolFactory
from ion.data.datastore.registry import LCStates as LCS


instrument_commands = (
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

ci_commands = (
    "start_direct_access",
    "stop_direct_access",
)

"""
Maybe some day these values are looked up from a registry of common
controlled vocabulary
"""
instrument_parameters = (
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

ci_parameters = (
    
)


class SBE49InstrumentDriver(InstrumentDriver):
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
        if command in instrument_commands:
            return (1, command)
        else:
            return (0, command)

    def get_status(self, args):
        """
        Return the non-parameter and non-lifecycle status of the instrument.
        This may include a snippit of config or important summary of what the
        instrument may be doing...or even something else.
        @param args a list of arguments that may be given for the status
            retreival.
        @return Return a tuple of (status_code, dict)
        """
        return (1, {'result': 'a-ok'})

class SBE49InstrumentAgent(InstrumentAgent):
    """
    Sea-Bird 49 specific instrument driver
    Inherits basic get, set, getStatus, getCapabilities, etc. from parent
    """
    driver = SBE49InstrumentDriver()

    @staticmethod
    def __translator(input):
        """
        A function (to be returned upon request) that will translate the
        very raw data from the instrument into the common archive format
        """
        return input

    @defer.inlineCallbacks
    def op_get_translator(self, content, headers, msg):
        """
        Return the translator function that will convert the very raw format
        of the instrument into a common OOI repository-ready format
        """
        yield self.reply_err(msg, "Not Implemented!")
#        yield self.reply_ok(msg, self.__translator)

    @defer.inlineCallbacks
    def op_get_capabilities(self, content, headers, msg):
        """
        Obtain a list of capabilities that this instrument has. This is
        simply a command and parameter list at this point
        """
        yield self.reply(msg, 'get_capabilities',
                         {IA.instrument_commands: instrument_commands,
                          IA.instrument_parameters: instrument_parameters,
                          IA.ci_commands: ci_commands,
                          IA.ci_parameters: ci_parameters}, {})

# Spawn of the process using the module name
factory = ProtocolFactory(SBE49InstrumentAgent)
