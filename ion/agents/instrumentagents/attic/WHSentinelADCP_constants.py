#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/WHSentinelADCP_constants.py
@author Bill Bollenbacher
@brief Constants associated with Workhorse ADCP instruments. These are to be
       shared between the instrument driver and instrument agent.
"""

class instrument_prompts:
    ADCP_LINE_TERMINATOR = "\r\n"
    DRIVER_LINE_TERMINATOR = "\r"
    PROMPT_INST = "\r\n"
    INST_PROMPT = ">"

instrument_commands = (
    "start",
    "stop",
    "break",
    "cf",
    "ck",
    "cr",
    "cs",
    "cz",
    "ea",
    "ed",
    "es",
    "ex",
    "expertoff",
    "experton",
    "ez",
    "ol",
    "pa",
    "pc1",
    "pc2",
    "ps0",
    "ps3",
    "te",
    "tp",
    "tpr",
    "wb",
    "wn",
    "wp",
    "ws"
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
)

"""
Some generalized instrument items added by instrument_agent module
"""
ci_parameters = (

)
