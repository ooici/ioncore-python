#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/instrument_agent.py
@author Steve Foley
@brief Instrument Agent, Driver, and Client class definitions
"""
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.data.dataobject import ResourceDescription
from ion.agents.resource_agent import ResourceAgent
from ion.agents.resource_agent import ResourceAgentClient

"""
Constants/Enumerations for tags in capabiltiies dict structures
"""
ci_commands = 'ci_commands'
ci_parameters = 'ci_parameters'
instrument_commands = 'instrument_commands'
instrument_parameters = 'instrument_parameters'


class InstrumentDriver():
    """
    A base driver class. This is intended to provide the common parts of
    the interface that instrument drivers should follow in order to use
    common InstrumentAgent methods. This should never really be instantiated.
    """
    def fetch_param(self, param):
        """
        Using the instrument protocol, fetch a parameter from the instrument
        @param param The parameter to fetch
        @retval The value of the fetched parameter
        """
        
    def set_param(self, param, value):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param param The parameter to set
        @param value The value to set
        @retval A small dict of parameter and value on success, empty dict on
            failure
        """
        
    def execute(self, command):
        """
        Using the instrument protocol, execute the requested command
        @param command The command to execute
        @retval Result code of some sort
        """
    
class InstrumentAgent(ResourceAgent):
    """
    The base class for developing Instrument Agents. This defines
    the interface to use for an instrument agent and some boiler plate
    function implementations that may be good enough for most agents.
    Child instrument agents should supply instrument-specific getTranslator
    and getCapabilities routines.
    """
    
    driver = None
        
    @defer.inlineCallbacks
    def op_get_translator(self, content, headers, msg):
        """
        Obtain a function that translates the raw data format of the actual
        device into a generic data format that can be sent to the repository.
        This should be a stub that is subclassed by the specific IA, possibly
        even the driver.
        """
        yield self.reply_ok(msg, lambda s: s)
        
    @defer.inlineCallbacks
    def op_get(self, content, headers, msg):
        """
        React to a request for parameter values. In the InstrumentAgent,
        this defaults to parameters relating to the CI side of operations
        For instrument parameters, use the op_getFromInstrument() methods
        @see op_getFromInstrument()
        @see op_getFromCI()
        @retval A reply message containing a dictionary of name/value pairs
        """
        yield self.op_get_from_CI(content, headers, msg)
    
    @defer.inlineCallbacks
    def op_get_from_instrument(self, content, headers, msg):
        """
        Get configuration parameters from the instrument side of the agent.
        This is stuff that would generally be handled by the instrument driver.
        @retval A reply message containing a dictionary of name/value pairs
        """
        assert(isinstance(content, list))
        assert(self.driver != None)
        response = {}
        for key in content:
            response[key] = self.driver.fetch_param(key)
        if response != {}:
            yield self.reply_ok(msg, response)
        else:
            yield self.reply_err(msg, 'No values found')
    
    @defer.inlineCallbacks
    def op_get_from_CI(self, content, headers, msg):
        """
        Get data from the cyberinfrastructure side of the agent (registry info,
        topic locations, messaging parameters, process parameters, etc.)
        @retval A reply message containing a dictionary of name/value pairs
        @todo Write this or push to subclass
        """
        assert(isinstance(content, list))
        assert(self.driver != None)
        response = {}
        #get data somewhere, or just punt this lower in the class hierarchy
        if response != {}:
            yield self.reply_ok(msg, response)
        else:
            yield self.reply_err(msg, 'No values found')
            
    @defer.inlineCallbacks    
    def op_set(self, content, headers, msg):
        """
        Set parameters to the infrastructure side of the agent. For
        instrument-specific values, use op_setToInstrument().
        @see op_setToInstrument
        @see op_setToCI
        @retval Message with a list of settings
            that were changed and what their new values are upon success.
            On failure, return the bad key, but previous keys were already set
        """
        yield self.op_set_to_CI(content, headers, msg)
    
    @defer.inlineCallbacks
    def op_set_to_instrument(self, content, headers, msg):
        """
        Set parameters to the instrument side of of the agent. These are
        generally values that will be handled by the instrument driver.
        @retval Message with a list of settings
            that were changed and what their new values are upon success.
            On failure, return the bad key, but previous keys were already set
        """
        assert(isinstance(content, dict))
        assert(self.driver != None)
        response = {}
        for key in content:
            result = {}
            result = self.driver.set_param(key, content[key])
            if result == {}:
                yield self.reply_err(msg, "Could not set %s" % key)
            else:
                response.update(result)
        assert(response != {})
        yield self.reply_ok(msg, response)
    
    @defer.inlineCallbacks
    def op_set_to_CI(self, content, headers, msg):
        """
        Set parameters related to the infrastructure side of the agent
        (registration information, location, network addresses, etc.)
        @retval Message with a list of settings that were changed and what
           their new values are upon success. On failure, return the bad key,
           but previous keys were already set
        @todo Write this or pass through to a subclass
        """
        pass
    
#    @defer.inlineCallbacks
#    def op_getLifecycleState(self, content, headers, msg):
#        """
#        Query the lifecycle state of the object
#        @retval Message with the lifecycle state
#        @todo Should actually work with registry for lifecycle state
#        """
#        yield self.reply(msg, 'getLifecycleState', self.lifecycleState, {})
        
#    @defer.inlineCallbacks
#    def op_setLifecycleState(self, content, headers, msg):
#        """
#        Set the lifecycle state
#        @retval Message with the lifecycle state that was set
#        @todo Should actually work with registry for lifecycle state
#        """
#       self.lifecycleState = content
#        yield self.reply(msg, 'setLifecycleState', content, {})
    
    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a specific command on the instrument, reply with a confirmation
        message including output of command, or simple ACK that command
        was executed. For InstrumentAgent calls, execute maps to an execution
        to the CI set of commands.
        @param content Should be a list where first element is the command,
            the rest are the arguments
        @retval ACK message with response on success, ERR message with string
            indicating code and response message on fail
        """
        yield self.op_execute_CI(content, headers, msg)
        
    @defer.inlineCallbacks
    def op_execute_CI(self, content, headers, msg):
        """
        Execute infrastructure commands related to the Instrument Agent
        instance. This includes commands for messaging, resource management
        processes, etc.
        @param content Should be a list where first element is the command,
            the rest are the arguments
        @retval ACK message with response on success, ERR message with string
            indicating code and response message on fail
        """
    
    @defer.inlineCallbacks
    def op_execute_instrument(self, content, headers, msg):
        """
        Execute instrument commands relate to the instrument fronted by this
        Instrument Agent. These commands will likely be handled by the
        underlying driver.
        @param content Should be a list where first element is the command,
            the rest are the arguments
        @retval ACK message with response on success, ERR message with string
            indicating code and response message on fail
        """
        assert(isinstance(content, unicode))
        assert(self.driver != None)
        execResult = self.driver.execute(content)
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
    def op_get_status(self, content, headers, msg):
        """
        Obtain the status of an instrument. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param content A list of arguments to make up the status request
        @retval ACK message with response on success, ERR message on failure
        """
        assert(isinstance(content, list))
        assert(self.driver != None)
        response = self.driver.get_status(content)
        if ((isinstance(response, tuple)) and (response[0] == 1)):
            yield self.reply_ok(msg, response[1])
        else:
            yield self.reply_err(msg, 'No values found')
            
class InstrumentAgentClient(ResourceAgentClient):
    """
    The base class for an Instrument Agent Client. It is a service
    that allows for RPC messaging
    """

    @defer.inlineCallbacks    
    def get_from_instrument(self, paramList):
        """
        Obtain a list of parameter names from the instrument
        @param paramList A list of the values to fetch
        @retval A dict of the names and values requested
        """
        assert(isinstance(paramList, list))
        (content, headers, message) = yield self.rpc_send('get_from_instrument',
                                                          paramList)
        assert(isinstance(content, dict))
        defer.returnValue(content)
    
    @defer.inlineCallbacks    
    def get_from_CI(self, paramList):
        """
        Obtain a list of parameter names from the instrument
        @param paramList A list of the values to fetch
        @retval A dict of the names and values requested
        """
        assert(isinstance(paramList, list))
        (content, headers, message) = yield self.rpc_send('get_from_CI',
                                                          paramList)
        assert(isinstance(content, dict))
        defer.returnValue(content)
    
    @defer.inlineCallbacks
    def set_to_instrument(self, paramDict):
        """
        Set a collection of values on an instrument
        @param paramDict A dict of parameter names and the values they are
            being set to
        @retval A dict of the successful set operations that were performed
        @todo Add exceptions for error conditions
        """
        assert(isinstance(paramDict, dict))
        (content, headers, message) = yield self.rpc_send('set_to_instrument',
                                                          paramDict)
        assert(isinstance(content, dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_to_CI(self, paramDict):
        """
        Set a collection of values on an instrument
        @param paramDict A dict of parameter names and the values they are
            being set to
        @retval A dict of the successful set operations that were performed
        @todo Add exceptions for error conditions
        """
        assert(isinstance(paramDict, dict))
        (content, headers, message) = yield self.rpc_send('set_to_CI',
                                                          paramDict)
        assert(isinstance(content, dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def execute_instrument(self, commandList):
        """
        Execute the instrument commands in the order of the list.
        Processing will cease when a command fails, but will not roll back.
        For instrument calls, use executeInstrument()
        @see executeInstrument()
        @param command_list An ordered list of commands to execute
        @retval Dictionary of responses to each execute command
        @todo Alter semantics of this call as needed...maybe arguments?
        @todo Add exceptions as needed
        """
        result = {}
        assert(isinstance(commandList, list))
        for command in commandList:
            (content, headers, message) = \
                yield self.rpc_send('execute_instrument', command)
            result[command] = content
        assert(isinstance(result, dict))
        defer.returnValue(result)
        
    @defer.inlineCallbacks
    def execute_CI(self, commandList):
        """
        Execute the instrument commands in the order of the list.
        Processing will cease when a command fails, but will not roll back.
        For instrument calls, use executeCI()
        @see executeInstrument()
        @param command_list An ordered list of commands to execute
        @retval Dictionary of responses to each execute command
        @todo Alter semantics of this call as needed...maybe arguments?
        @todo Add exceptions as needed
        """
        result = {}
        assert(isinstance(commandList, list))
        for command in commandList:
            (content, headers, message) = yield self.rpc_send('execute_CI',
                                                              command)
            result[command] = content
        assert(isinstance(result, dict))
        defer.returnValue(result)
        
    @defer.inlineCallbacks
    def get_status(self, argList):
        """
        Obtain the non-parameter and non-lifecycle status of the instrument
        """
        assert(isinstance(argList, list))
        (content, headers, message) = yield self.rpc_send('get_status',
                                                              argList)
        assert(isinstance(content, dict))
        defer.returnValue(content)
        
    @defer.inlineCallbacks    
    def get_capabilities(self):
        """
        Obtain a list of capabilities from the instrument
        @retval A dict with commands and parameter lists that are supported
            such as {'commands':[], 'parameters':[]}
        """
        (content, headers, message) = yield self.rpc_send('get_capabilities',
                                                          ())
        assert(isinstance(content, dict))
        assert('ci_commands' in content)
        assert('ci_parameters' in content)
        assert(isinstance(content['ci_commands'], list))
        assert(isinstance(content['ci_parameters'], list))
        assert('instrument_commands' in content)
        assert('instrument_parameters' in content)
        assert(isinstance(content['instrument_commands'], list))
        assert(isinstance(content['instrument_parameters'], list))

        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def get_translator(self):
        """
        Get the translator routine that will convert the raw format of the
        device into a common, repository-ready one
        @retval Function code that takes in data in the streamed format and
            and puts it into the repository ready format
        """
        (content, headers, message) = yield self.rpc_send('get_translator', ())
        #assert(inspect.isroutine(content))
        defer.returnValue(content)  