"""
@author Edward Hunter
@brief Python Capability Container shell functions for interactive
    instrument agent and driver testing.
"""

"""
USE. From the CC shell, import asa commands and constants.

bin/twistd -n cc -h amoeba.ucsd.edu

import ion.agents.instrumentagents.agent_shell_api as asa
from ion.agents.instrumentagents.instrument_constants import *

Start agent and client, cycle into observatory mode, query agent and
device parameters, start autosampling, stop autosampling, cycle to
uninitialized:

asa.start_agent()
asa.init()
asa.go_active()
asa.run()
asa.get_obs()
asa.start()
asa.stop()
asa.reset()
"""

from twisted.internet import defer

from ion.core import ioninit
from ion.core.process.process import ProcessDesc

from ion.agents.instrumentagents import instrument_agent as ia
from ion.agents.instrumentagents.instrument_constants import *

AGENT_MODULE = 'ion.agents.instrumentagents.instrument_agent'
IAC = None


sbe_host = '137.110.112.119'
sbe_port = 4001    
driver_config = {
    'ipport':sbe_port,
    'ipaddr':sbe_host
}
agent_config = {}

# Process description for the SBE37 driver.
driver_desc = {
    'name':'SBE37_driver',
    'module':'ion.agents.instrumentagents.SBE37_driver',
    'class':'SBE37Driver',
    'spawnargs':{'config':driver_config}
}

# Process description for the SBE37 driver client.
driver_client_desc = {
    'name':'SBE37_client',
    'module':'ion.agents.instrumentagents.SBE37_driver',
    'class':'SBE37DriverClient',
    'spawnargs':{}
}

params_driver = {
    'AGENT_PARAM_DRIVER_CONFIG':driver_config,
    'AGENT_PARAM_DRIVER_DESC':driver_desc,
    'AGENT_PARAM_DRIVER_CLIENT_DESC':driver_client_desc
}

params_acq = {
    (DriverChannel.INSTRUMENT,'NAVG'):1,
    (DriverChannel.INSTRUMENT,'INTERVAL'):5,
    (DriverChannel.INSTRUMENT,'OUTPUTSV'):True,
    (DriverChannel.INSTRUMENT,'OUTPUTSAL'):True,
    (DriverChannel.INSTRUMENT,'TXREALTIME'):True,
    (DriverChannel.INSTRUMENT,'STORETIME'):True
}

@defer.inlineCallbacks
def get_obs(params=None):
    if not params:
        params = ['all']
    result = yield IAC.get_observatory(params)
    defer.returnValue(result)
    
@defer.inlineCallbacks
def set_obs_driver():
    result = yield IAC.set_observatory(params_driver,'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def set_obs_acq():
    result = yield IAC.set_observatory(params_acq,'create')
    defer.returnValue(result)
    
@defer.inlineCallbacks
def get_obs_status(params=None):
    if not params:
        params = ['all']
    result = yield IAC.get_observatory_status(params)
    defer.returnValue(result)

@defer.inlineCallbacks
def init():
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.INITIALIZE],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def go_active():
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.GO_ACTIVE],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def run():
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.RUN],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def reset():
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.RESET],'create')
    defer.returnValue(result)
    
@defer.inlineCallbacks
def get_device(params=None):
    if not params:
        params = [('all','all')]
    result = yield IAC.get_device(params)
    defer.returnValue(result)
    
@defer.inlineCallbacks
def acquire():
    result = yield IAC.execute_device([DriverChannel.INSTRUMENT],
        [DriverCommand.ACQUIRE_SAMPLE],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def start():
    result = yield IAC.execute_device([DriverChannel.INSTRUMENT],
        [DriverCommand.START_AUTO_SAMPLING],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def stop():
    result = yield IAC.execute_device([DriverChannel.INSTRUMENT],
        [DriverCommand.STOP_AUTO_SAMPLING],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def start_agent():
    global IAC
    sup = yield ioninit.container_instance.proc_manager.create_supervisor()
    yield sup.spawn_child(ProcessDesc(name=AGENT_MODULE, module=AGENT_MODULE))

    for item in sup.child_procs:
        if item.proc_module == AGENT_MODULE:
            IAC = ia.InstrumentAgentClient(target=item.proc_id)
            yield IAC.set_observatory(params_driver,'create')
            print 'Created client!'







