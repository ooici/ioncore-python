#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE49.py
@author Steve Foley
@brief CI interface for SeaBird SBE-49 CTD
"""
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.agents.instrumentagents import instrument_agent as IA
from ion.agents.instrumentagents.instrument_agent import InstrumentAgent

from ion.core.base_process import ProtocolFactory, ProcessDesc
from ion.core import bootstrap

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

# Gotta have this AFTER the "static" variables above
from ion.agents.instrumentagents.SBE49_instrument_driver import SBE49InstrumentDriverClient
    
class SBE49InstrumentAgent(InstrumentAgent):
    """
    Sea-Bird 49 specific instrument driver
    Inherits basic get, set, getStatus, getCapabilities, etc. from parent
    """
    
    @defer.inlineCallbacks
    def plc_init(self):
        """
        Initialize instrument driver when this process is started.
        """
        pd = ProcessDesc(**{'name':'SBE49Driver',
                          'module':'ion.agents.instrumentagents.SBE49_instrument_driver',
                          'class':'SBE49InstrumentDriver'})
        self.sup = yield bootstrap.create_supervisor()
                
        driver_id = yield self.sup.spawn_child(pd)
        self.driver_client = SBE49InstrumentDriverClient(proc=self.sup,
                                                         target=driver_id)

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
