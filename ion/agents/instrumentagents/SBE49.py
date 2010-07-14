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
from ion.agents.instrumentagents.instrument_agent import InstrumentDriverClient

from ion.core.base_process import ProtocolFactory

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
    __instrument_parameters = {
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

    @defer.inlineCallbacks
    def op_fetch_params(self, content, headers, msg):
        """
        Operate in instrument protocol to get parameter
        @todo Write the code to interface this with something
        """
        assert(isinstance(content, list))
        result = {}
        for param in content:
            result[param] = self.__instrument_parameters[param]
        yield self.reply_ok(msg, result)

    @defer.inlineCallbacks
    def op_set_params(self, content, headers, msg):
        """
        operate in instrument protocol to set a parameter
        """
        assert(isinstance(content, dict))
        updated = False
        for param in content:
            if (param in self.__instrument_parameters):
                self.__instrument_parameters[param] = content[param]
                updated = True
        if (updated == True):
            yield self.reply_ok(msg, content)
        else:
            yield self.reply_err(msg, "No values updated")
            
    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute the given command structure (first element command, rest
        of the elements are arguments)
        """
        assert(isinstance(content, list))
        command = content[0]
        if command in instrument_commands:
            yield self.reply_ok(msg, command)
        else:
            yield self.reply_err(msg, "Invalid Command")

    @defer.inlineCallbacks
    def op_get_status(self, content, headers, msg):
        """
        Return the non-parameter and non-lifecycle status of the instrument.
        This may include a snippit of config or important summary of what the
        instrument may be doing...or even something else.
        @param args a list of arguments that may be given for the status
            retreival.
        @return Return a tuple of (status_code, dict)
        @todo Remove this? Is it even used?
        """
        yield self.reply_ok(msg, "a-ok")
        
    @defer.inlineCallbacks
    def op_configure_driver(self, content, headers, msg):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param content A dict with parameters for the driver
        @todo Actually make this stub do something
        """
        logging.debug("*** configuring driver in driver, Content: %s", content)
        assert(isinstance(content, dict))
        # Do something here, then adjust test case
        yield self.reply_ok(msg, content)
        
        
class SBE49InstrumentDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """
    
    
class SBE49InstrumentAgent(InstrumentAgent):
    """
    Sea-Bird 49 specific instrument driver
    Inherits basic get, set, getStatus, getCapabilities, etc. from parent
    """
    
    def __init__(self):
        self.driver = SBE49InstrumentDriver()
        self.driver_id = yield self.driver.spawn()
        self.driver_client = SBE49InstrumentDriverClient(proc=self.sup,
                                                         target=self.driver_id)

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
