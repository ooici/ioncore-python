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
asa.get_device()
asa.start()
asa.stop()
asa.reset()

In order to see stdio prints of state transitions and other activity
in the driver and agent, add this line to your shell setup script:
export DEBUG_PRINT=True
otherwise, only the message replies will appear in the shell.
"""

from twisted.internet import defer

from ion.core import ioninit
from ion.core.process.process import ProcessDesc

from ion.agents.instrumentagents import instrument_agent as ia
from ion.agents.instrumentagents.instrument_constants import *

AGENT_MODULE = 'ion.agents.instrumentagents.instrument_agent'
IAC = None

# The default driver configuration.
sbe_host = '137.110.112.119'
sbe_port = 4001    
driver_config = {
    'ipport':sbe_port,
    'ipaddr':sbe_host
}

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
    """
    Get agent observatory parameters.
    """
    if not params:
        params = [AgentParameter.ALL]
    result = yield IAC.get_observatory(params)
    defer.returnValue(result)

@defer.inlineCallbacks
def set_obs(params):
    """
    Set agent observatory parameters. Can set driver configuration.
    """
    result = yield IAC.set_observatory(params,'create')
    defer.returnValue(result)
    
@defer.inlineCallbacks
def get_cap(params=None):
    """
    Get agent capabilities.
    """
    if not params:
        params = [InstrumentCapability.ALL]
    result = yield IAC.get_capabilities(params)
    defer.returnValue(result)
    
@defer.inlineCallbacks
def get_obs_status(params=None):
    """
    Get agent statuses.
    """
    if not params:
        params = [AgentStatus.ALL]
    result = yield IAC.get_observatory_status(params)
    defer.returnValue(result)

"""
@defer.inlineCallbacks
def set_obs_driver():
    #Set driver configuation to the default value.
    result = yield IAC.set_observatory(params_driver,'create')
    defer.returnValue(result)
"""

"""
@defer.inlineCallbacks
def set_obs_acq():
    #Set agent 
    result = yield IAC.set_observatory(params_acq,'create')
    defer.returnValue(result)
"""

@defer.inlineCallbacks
def init():
    """
    Transition to intialized state. This creates driver process and client.
    """
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.INITIALIZE],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def go_active():
    """
    Transitioin to active state. This established communications
    between driver and hardware.
    """
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.GO_ACTIVE],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def run():
    """
    Transition to observatory mode.
    """
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.RUN],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def reset():
    """
    Transition to inactive state. This closes comms with the device and
    destroys the driver objects.
    """
    result = yield IAC.execute_observatory([AgentCommand.TRANSITION,
                                    AgentEvent.RESET],'create')
    defer.returnValue(result)
    
@defer.inlineCallbacks
def get_device(params=None):
    """
    Get device parameters.
    """
    if not params:
        params = [(DriverChannel.ALL,DriverParameter.ALL)]
    result = yield IAC.get_device(params)
    defer.returnValue(result)

@defer.inlineCallbacks
def set_device(params):
    """
    Set device parameters.
    """
    result = yield IAC.set_device(params,'create')
    defer.returnValue(result)
    
@defer.inlineCallbacks
def acquire():
    """
    Poll for a sample.
    """
    result = yield IAC.execute_device([DriverChannel.INSTRUMENT],
        [DriverCommand.ACQUIRE_SAMPLE],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def start():
    """
    Start autosampling.
    """
    result = yield IAC.execute_device([DriverChannel.INSTRUMENT],
        [DriverCommand.START_AUTO_SAMPLING],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def stop():
    """
    Stop autosampling.
    """
    result = yield IAC.execute_device([DriverChannel.INSTRUMENT],
        [DriverCommand.STOP_AUTO_SAMPLING,'GETDATA'],'create')
    defer.returnValue(result)

@defer.inlineCallbacks
def start_agent():
    """
    Create the unitialized agent process and client. Set default
    driver parameters.
    """
    global IAC
    sup = yield ioninit.container_instance.proc_manager.create_supervisor()
    yield sup.spawn_child(ProcessDesc(name=AGENT_MODULE, module=AGENT_MODULE))

    for item in sup.child_procs:
        if item.proc_module == AGENT_MODULE:
            IAC = ia.InstrumentAgentClient(target=item.proc_id)
            yield IAC.set_observatory(params_driver,'create')
            print 'Created client!'







