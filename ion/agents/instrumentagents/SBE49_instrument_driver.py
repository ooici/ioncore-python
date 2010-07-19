#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE49_instrument_driver.py
@author Steve Foley
@brief Driver code for SeaBird SBE-49 CTD
"""
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.agents.instrumentagents.instrument_agent import InstrumentDriver
from ion.agents.instrumentagents.instrument_agent import InstrumentDriverClient
from ion.agents.instrumentagents.SBE49 import instrument_commands


from ion.core.base_process import ProtocolFactory

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
        @todo actually do something
        """
        ok = True
        assert(isinstance(content, dict))
        if (content == {}):
            yield self.reply_err(msg, "Empty command")
        for command in content.keys():
            if command not in instrument_commands:
                yield self.reply_err(msg, "Invalid Command")
        yield self.reply_ok(msg, content.keys())

    
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
        assert(isinstance(content, dict))
        # Do something here, then adjust test case
        yield self.reply_ok(msg, content)
        
        
class SBE49InstrumentDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """
    
# Spawn of the process using the module name
factory = ProtocolFactory(SBE49InstrumentDriver)