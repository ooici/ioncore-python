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
from ion.core.base_process import BaseProcess, BaseProcessClient
from ion.resources.ipaa_resource_descriptions import InstrumentAgentResourceInstance

"""
Constants/Enumerations for tags in capabiltiies dict structures
"""
ci_commands = 'ci_commands'
ci_parameters = 'ci_parameters'
instrument_commands = 'instrument_commands'
instrument_parameters = 'instrument_parameters'

# CI parameter key constant
driver_address = 'driver_address'


class InstrumentDriver(BaseProcess):
    """
    A base driver class. This is intended to provide the common parts of
    the interface that instrument drivers should follow in order to use
    common InstrumentAgent methods. This should never really be instantiated.
    """
    def op_fetch_params(self, content, headers, msg):
        """
        Using the instrument protocol, fetch a parameter from the instrument
        @param content A list of parameters to fetch
        @retval A dictionary with the parameter and value of the requested
            parameter
        """

    def op_set_params(self, content, headers, msg):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param content A dictionary with the parameters and values to set
        @retval A small dict of parameter and value on success, empty dict on
            failure
        """

    def op_execute(self, content, headers, msg):
        """
        Using the instrument protocol, execute the requested command
        @param command A dictionary where the command name is the key and the
            arguments are the value as a (possibly empty) list. For example:
            {'command1':['arg1', 'arg2'], 'command2':[]}
        @retval Result code of some sort
        """

    def op_configure_driver(self, content, headers, msg):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param content A dict with parameters for the driver
        """

    def op_disconnect(self, content, headers, msg):
        """
        Disconnect from the instrument
        @param none
        """

class InstrumentDriverClient(BaseProcessClient):
    """
    The base class for the instrument driver client interface. This interface
    is designed to be used by the instrument agent to work with the driver.
    """

    @defer.inlineCallbacks
    def fetch_params(self, param_list):
        """
        Using the instrument protocol, fetch a parameter from the instrument
        @param param_list A list or tuple of parameters to fetch
        @retval A dictionary with the parameter and value of the requested
            parameter
        """
        assert(isinstance(param_list, (list, tuple)))
        (content, headers, message) = yield self.rpc_send('fetch_params',
                                                          param_list)
        assert(isinstance(content, dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_params(self, param_dict):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param param_dict A dictionary with the parameters and values to set
        @retval A small dict of parameter and value on success, empty dict on
            failure
        """
        assert(isinstance(param_dict, dict))
        (content, headers, message) = yield self.rpc_send('set_params',
                                                          param_dict)
        assert(isinstance(content, dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def execute(self, command):
        """
        Using the instrument protocol, execute the requested command
        @param command A dictionary where the command name is the key and the
            arguments are the value as a (possibly empty) list. For example:
            {'command1':['arg1', 'arg2'], 'command2':[]}
        @retval Result code of some sort
        """
        assert(isinstance(command, dict))
        (content, headers, message) = yield self.rpc_send('execute',
                                                          command)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_status(self, arg):
        """
        Using the instrument protocol, gather status from the instrument
        @param arg The argument needed for gathering status
        @retval Result message of some sort
        """
        (content, headers, message) = yield self.rpc_send('get_status', arg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def configure_driver(self, config_vals):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param config_vals A dict with parameters for the driver
        """
        assert(isinstance(config_vals, dict))
        (content, headers, message) = yield self.rpc_send('configure_driver',
                                                          config_vals)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def initialize(self, arg):
        """
        Disconnect from the instrument
        @param none
        @retval Result code of some sort
        """
        #assert(isinstance(command, dict))
        logging.debug("DHE: in initialize!")
        (content, headers, message) = yield self.rpc_send('initialize',
                                                          arg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def disconnect(self, command):
        """
        Disconnect from the instrument
        @param none
        @retval Result code of some sort
        """
        #assert(isinstance(command, dict))
        logging.debug("DHE: in disconnect!")
        (content, headers, message) = yield self.rpc_send('disconnect',
                                                          command)
        defer.returnValue(content)


class InstrumentAgent(ResourceAgent):
    """
    The base class for developing Instrument Agents. This defines
    the interface to use for an instrument agent and some boiler plate
    function implementations that may be good enough for most agents.
    Child instrument agents should supply instrument-specific getTranslator
    and getCapabilities routines.
    """

    driver_client = None

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
        assert(isinstance(content, (list, tuple)))
        assert(self.driver_client != None)
        response = {}
        for key in content:
            response = yield self.driver_client.fetch_params(content)
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
        assert(isinstance(content, (list, tuple)))
        assert(self.driver_client != None)
        response = {}

        # get data somewhere, or just punt this lower in the class hierarchy
        if ("driver_address" in content):
            response['driver_address'] = str(self.driver_client.target)

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
        @param content A dict that contains the key:value pair to set
        @retval Message with a list of settings
            that were changed and what their new values are upon success.
            On failure, return the bad key, but previous keys were already set
        """
        assert(isinstance(content, dict))
        response = {}
        result = yield self.driver_client.set_params(content)
        if result == {}:
            yield self.reply_err(msg, "Could not set %s" % content)
            return
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

    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute a command on the instrument, reply with a confirmation
        message including output of command, or simple ACK that command
        was executed. For InstrumentAgent calls, execute maps to an execution
        to the CI set of commands.
        @param command A dictionary where the command name is the key and the
            arguments are the value as a (possibly empty) list. For example:
            {'command1':['arg1', 'arg2'], 'command2':[]}
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
    def op_disconnect(self, content, headers, msg):
        """
        Disconnect from the instrument.
        @param none
        @return ACK message with response on success, ERR message with string
            indicating code and response message on fail
        """
        assert(isinstance(content, list))
        assert(self.driver != None)
        execResult = self.driver.disconnect(content)
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
    def op_execute_instrument(self, content, headers, msg):
        """
        Execute instrument commands relate to the instrument fronted by this
        Instrument Agent. These commands will likely be handled by the
        underlying driver.
        @param command A dictionary where the command name is the key and the
            arguments are the value as a (possibly empty) list. For example:
            {'command1':['arg1', 'arg2'], 'command2':[]}
        @retval ACK message with response on success, ERR message with string
            indicating code and response message on fail
        """
        assert(isinstance(content, dict))
        execResult = yield self.driver_client.execute(content)
        assert(isinstance(execResult, dict))
        if (execResult['status'] == 'OK'):
            yield self.reply_ok(msg, execResult['value'])
        else:
            yield self.reply_err(msg, "Failure, response is: %s"
                                 % execResult['value'])
    @defer.inlineCallbacks
    def op_get_status(self, content, headers, msg):
        """
        Obtain the status of an instrument. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param content A list of arguments to make up the status request
        @retval ACK message with response on success, ERR message on failure
        """
        assert(isinstance(content, (list, tuple)))
        response = yield self.driver_client.get_status(content)
        if (response['status'] == 'OK'):
            yield self.reply_ok(msg, response['value'])
        else:
            yield self.reply_err(msg, response['value'])

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
    def disconnect(self, argList):
        """
        Disconnect from the instrument
        """
        assert(isinstance(argList, list))
        (content, headers, message) = yield self.rpc_send('disconnect',
                                                              argList)
        assert(isinstance(content, dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def execute_instrument(self, command):
        """
        Execute the instrument commands in the order of the list.
        Processing will cease when a command fails, but will not roll back.
        For instrument calls, use executeInstrument()
        @see executeInstrument()
        @param command A dictionary where the command name is the key and the
            arguments are the value as a (possibly empty) list. For example:
            {'command1':['arg1', 'arg2'], 'command2':[]}
        @retval Dictionary of responses to each execute command
        @todo Alter semantics of this call as needed...maybe a list?
        @todo Add exceptions as needed
        """
        assert(isinstance(command, dict))
        (content, headers, message) = yield self.rpc_send('execute_instrument',
                                                          command)
        defer.returnValue(content)

    def execute_CI(self, command):
        """
        Execute the instrument commands in the order of the list.
        Processing will cease when a command fails, but will not roll back.
        For instrument calls, use executeCI()
        @see executeInstrument()
        @param command A dictionary where the command name is the key and the
            arguments are the value as a (possibly empty) list. For example:
            {'command1':['arg1', 'arg2'], 'command2':[]}
        @retval Dictionary of responses to each execute command
        @todo Alter semantics of this call as needed...maybe a command list?
        @todo Add exceptions as needed
        """
        assert(isinstance(command, list))
        (content, headers, message) = yield self.rpc_send('execute_CI',
                                                          command)
        assert(isinstance(content, dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_status(self, argList):
        """
        Obtain the non-parameter and non-lifecycle status of the instrument
        @param argList A list of arguments to pass for status
        """
        assert(isinstance(argList, list))
        (content, headers, message) = yield self.rpc_send('get_status',
                                                              argList)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_capabilities(self):
        """
        Obtain a list of capabilities from the instrument
        @retval A dict with commands and
        parameter lists that are supported
            such as {'commands':(), 'parameters':()}
        """
        (content, headers, message) = yield self.rpc_send('get_capabilities',
                                                          ())
        assert(isinstance(content, dict))
        assert(ci_commands in content.keys())
        assert(ci_parameters in content.keys())
        assert(isinstance(content[ci_commands], (tuple, list)))
        assert(isinstance(content[ci_parameters], (tuple, list)))
        assert(instrument_commands in content.keys())
        assert(instrument_parameters in content.keys())
        assert(isinstance(content[instrument_commands], (tuple, list)))
        assert(isinstance(content[instrument_parameters], (tuple, list)))

        listified = {}
        for listing in content.keys():
            listified[listing] = list(content[listing])

        # Add in the special stuff that all instruments know
        listified[ci_parameters].append(driver_address)

        defer.returnValue(listified)

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

    @defer.inlineCallbacks
    def register_resource(self):
        """
        Register the resource. Since this is a subclass, make the appropriate
        resource description for the registry and pass that into the
        registration call.
        """
        ia_instance = InstrumentAgentResourceInstance()
        ci_params = yield self.get_from_CI([driver_address])
        ia_instance.driver_process_id = ci_params[driver_address]
        result = yield ResourceAgentClient.register_resource(self,
                                                             ia_instance)
        defer.returnValue(result)
