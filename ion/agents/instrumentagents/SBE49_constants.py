#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/SBE49_constants.py
@author Steve Foley
@brief Constants associated with SBE49 instruments. These are to be shared
    between the instrument driver and instrument agent.
"""

class instrument_prompts:
    LINE_TERM = "\r\n"
    PROMPT_INST = "\r\n"
    PROMPT_INST_BURST = "\r\n\r\n\r\n\r\n"
    #INST_SLEEPY_PROMPT = "\0SBE 37-SM\r\nS>"
    INST_SLEEPY_PROMPT = "\0SBE 37-SM\r\n"
    #INST_PROMPT = "\r\nS>"
    INST_PROMPT = "S>"
    INST_CONFUSED = "\r\n?cmd S>"
    INST_GONE_TO_SLEEP = "\0"

instrument_commands = (
    "setdefaults",
    "ds",
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

command_substitutions = {
    "StartAcquisition":"start"
}

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

"""
Some generalized instrument items added by instrument_agent module
"""
ci_parameters = (

)
