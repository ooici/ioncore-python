#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/instrument_agent.py
@author Steve Foley
@brief Instrument Agent, Driver, and Client class definitions
"""
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer

from ion.agents.resource_agent import ResourceAgent
from ion.agents.resource_agent import ResourceAgentClient

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
        @return The value of the fetched parameter
        """
        
    def set_param(self, param, value):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param param The parameter to set
        @param value The value to set
        @return A small dict of parameter and value on success, empty dict on
            failure
        """
        
    def execute(self, command):
        """
        Using the instrument protocol, execute the requested command
        @param command The command to execute
        @return Result code of some sort
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
    def op_getTranslator(self, content, headers, msg):
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
        React to a request for parameter values,
        @return A reply message containing a dictionary of name/value pairs
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
    def op_set(self, content, headers, msg):
        """
        Set parameters to the requested values.
        @return Message with a list of settings
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
    def op_getLifecycleState(self, content, headers, msg):
        """
        Query the lifecycle state of the object
        @return Message with the lifecycle state
        @todo Should actually work with registry for lifecycle state
        """
        yield self.reply(msg, 'getLifecycleState', self.lifecycleState, {})
        
    @defer.inlineCallbacks
    def op_setLifecycleState(self, content, headers, msg):
        """
        Set the lifecycle state
        @return Message with the lifecycle state that was set
        @todo Should actually work with registry for lifecycle state
        """
        self.lifecycleState = content
        yield self.reply(msg, 'setLifecycleState', content, {})
    
    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a specific command on the instrument, reply with a confirmation
        message including output of command, or simple ACK that command
        was executed.
        @param content Should be a list where first element is the command,
            the rest are the arguments
        @return ACK message with response on success, ERR message with string
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
    def op_getStatus(self, content, headers, msg):
        """
        Obtain the status of an instrument. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param content A list of arguments to make up the status request
        @return ACK message with response on success, ERR message on failure
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
    def get(self, paramList):
        """
        Obtain a list of parameter names from the instrument
        @param paramList A list of the values to fetch
        @return A dict of the names and values requested
        """
        assert(isinstance(paramList, list))
        (content, headers, message) = yield self.rpc_send('get', paramList)
        assert(isinstance(content, dict))
        defer.returnValue(content)
    
    @defer.inlineCallbacks
    def set(self, paramDict):
        """
        Set a collection of values on an instrument
        @param paramDict A dict of parameter names and the values they are
            being set to
        @return A dict of the successful set operations that were performed
        @todo Add exceptions for error conditions
        """
        assert(isinstance(paramDict, dict))
        (content, headers, message) = yield self.rpc_send('set', paramDict)
        assert(isinstance(content, dict))
        defer.returnValue(content)
    
    @defer.inlineCallbacks
    def execute(self, commandList):
        """
        Execute the commands in the order of the list. Processing will cease
        when a command fails, but will not roll back.
        @param command_list An ordered list of commands to execute
        @return Dictionary of responses to each execute command
        @todo Alter semantics of this call as needed...maybe arguments?
        @todo Add exceptions as needed
        """
        result = {}
        assert(isinstance(commandList, list))
        for command in commandList:
            (content, headers, message) = yield self.rpc_send('execute',
                                                              command)
            result[command] = content
        assert(isinstance(result, dict))
        defer.returnValue(result)

    @defer.inlineCallbacks
    def getStatus(self, argList):
        """
        Obtain the non-parameter and non-lifecycle status of the instrument
        """
        assert(isinstance(argList, list))
        (content, headers, message) = yield self.rpc_send('getStatus',
                                                              argList)
        assert(isinstance(content, dict))
        defer.returnValue(content)
        
    @defer.inlineCallbacks    
    def getCapabilities(self):
        """
        Obtain a list of capabilities from the instrument
        @return A dict with commands and parameter lists that are supported
            such as {'commands':[], 'parameters':[]}
        """
        (content, headers, message) = yield self.rpc_send('getCapabilities',
                                                          ())
        assert(isinstance(content, dict))
        assert('commands' in content)
        assert('parameters' in content)
        assert(isinstance(content['commands'], list))
        assert(isinstance(content['parameters'], list))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def getTranslator(self):
        """
        Get the translator routine that will convert the raw format of the
        device into a common, repository-ready one
        @return Function code that takes in data in the streamed format and
            and puts it into the repository ready format
        """
        (content, headers, message) = yield self.rpc_send('getTranslator', ())
        #assert(inspect.isroutine(content))
        defer.returnValue(content)  