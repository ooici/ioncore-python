#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/instrument_agent.py
@author Steve Foley
@brief Instrument Agent, Driver, and Client class definitions
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.agents.resource_agent import ResourceAgent
from ion.agents.resource_agent import ResourceAgentClient
from ion.agents.instrumentagents.phrase import Phrase, GetPhrase, SetPhrase, ExecutePhrase
from ion.agents.instrumentagents.phrase import GetAction, SetAction, ExecuteAction
from ion.core.exception import ReceivedError
from ion.services.dm.distribution.pubsub_service import PubSubClient
from ion.data.dataobject import ResourceReference, DataObject
from ion.core.process.process import Process, ProcessClient
from ion.resources.ipaa_resource_descriptions import InstrumentAgentResourceInstance
from ion.resources.dm_resource_descriptions import PublisherResource

"""
Constants/Enumerations for tags in capabilities dict structures
"""
ci_commands = 'ci_commands'
ci_parameters = 'ci_parameters'
instrument_commands = 'instrument_commands'
instrument_parameters = 'instrument_parameters'

# parameter names for all instrument agents
ci_param_list = {
    "DataTopics":"DataTopics",
    "EventTopics":"EventTopics",
    "StateTopics":"StateTopics",
    "DriverAddress":"DriverAddress"
}

publish_msg_type = {
    "Error":"Error",
    "StateChange":"StateChange",
    "ConfigChange":"ConfigChange",
    "Data":"Data",
    "Event":"Event"
}

# CI parameter key constant
driver_address = 'DriverAddress'


class InstrumentDriver(Process):
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
        @param command A list where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
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

class InstrumentDriverClient(ProcessClient):
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
        @param command An ordered list of lists where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval Result code of some sort
        """
        log.debug("Driver client executing command: %s", command)
        assert(isinstance(command, (list, tuple))), "Bad Driver client execute type"
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
        log.debug("DHE: in initialize!")
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
        log.debug("DHE: in IDC disconnect!")
        (content, headers, message) = yield self.rpc_send('disconnect',
                                                          command)
        defer.returnValue(content)

class InstrumentAgent(ResourceAgent):
    """
    The base class for developing Instrument Agents. This defines
    the interface to use for an instrument agent and some boiler plate
    function implementations that may be good enough for most agents.
    Child instrument agents should supply instrument-specific routines such as
    getCapabilities.
    """
    
    """
    The driver client to communicate with the child driver
    """
    driver_client = None
    
    """
    A dictionary of the topics where data is published, indexed by transducer
    name or "Device" for the whole device. Gets set initially by
    subclass, then at runtime by user as needed.
    """
    output_topics = None

    """
    A dictionary of the topics where events are published, indexed by
    transducer name or "Device" for the whole device. Gets set initially by
    subclass, then at runtime by user as needed.
    """
    event_topics = None

    """
    A dictionary of the topics where state changes are published, indexed by
    transducer name or "Device" for the whole device. Gets set initially by
    subclass, then at runtime by user as needed.
    """
    state_topics = None

    """
    A phrase to keep track of pending action lists. Should be some subclass of
    Phrase (GetPhrase, SetPhrase, ExecutePhrase)
    """
    pending_phrase = None
    
    def plc_init(self):
        ResourceAgent.plc_init(self)
        self.pubsub_client = PubSubClient(proc=self)
        
    @defer.inlineCallbacks
    def _register_publisher(self):
        publisher = PublisherResource.create("IA publisher", self,
            self.output_topics.values() + self.event_topics.values() + self.state_topics.values(),
            'DataObject')
        publisher = yield self.pubsub_client.define_publisher(publisher)

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
    def _action_get_from_device(self, action):
        """
        Execute an action associated with getting a value from an instrument.
        @param action A GetAction object with a destination of device
        @retval Tuple of success/fail boolean and the get_from_device return
            result or error message.
        """
        assert(isinstance(action, GetAction)), "Expected a GetAction object"
        assert(action.destination == Phrase.device), "Expected device destination"
        response = yield self.driver_client.fetch_params(action.struct)
        if response != {}:
            yield (True, "Success")
        else:
            yield (False, "ERROR: No item found")

    @defer.inlineCallbacks
    def op_get_observatory(self, content, headers, msg):
        """
        Get data from the cyberinfrastructure side of the agent (registry info,
        topic locations, messaging parameters, process parameters, etc.)
        @retval A reply message containing a dictionary of name/value pairs
        @todo Write this or push to subclass
        """
        #assert(isinstance(content, (list, tuple)))
        #assert(self.driver_client != None)
        response = {}
        # get data somewhere, or just punt this lower in the class hierarchy
        if (ci_param_list[driver_address] in content):
            response[ci_param_list[driver_address]] = str(self.driver_client.target)
        
        if (ci_param_list['DataTopics'] in content):
            response[ci_param_list['DataTopics']] = {}
            for i in self.output_topics.keys():
                response[ci_param_list['DataTopics']][i] = self.output_topics[i].encode()
        if (ci_param_list['StateTopics'] in content):
            response[ci_param_list['StateTopics']] = {}
            for i in self.state_topics.keys():
                response[ci_param_list['StateTopics']][i] = self.state_topics[i].encode()
        if (ci_param_list['EventTopics'] in content):
            response[ci_param_list['EventTopics']] = {}
            for i in self.event_topics.keys():
                response[ci_param_list['EventTopics']][i] = self.event_topics[i].encode()

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
    def op_execute_observatory(self, content, headers, msg):
        """
        Execute infrastructure commands related to the Instrument Agent
        instance. This includes commands for messaging, resource management
        processes, etc.
        @param command A list where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval ACK message with response on success, ERR message with string
            indicating code and response message on fail
        """
        if (self.pending_phrase != None):
            self.pending_phrase.add(ExecuteAction(Phrase.observatory, content))
            yield self.reply_ok(msg, "STUB of execute_observatory with phrase")
        else:
            yield self.reply_ok(msg, "STUB...execute observatory with no phrase")
            
    @defer.inlineCallbacks
    def op_disconnect(self, content, headers, msg):
        """
        Disconnect from the instrument.
        @param none
        @return ACK message with response on success, ERR message with string
            indicating code and response message on fail
        """
        log.debug("DHE: IA in op_disconnect!")
        assert(isinstance(content, list))
        assert(self.driver != None)
        execResult = self.driver.disconnect(content)
        assert(len(execResult) == 2)
        (errorCode, response) = execResult
        assert(isinstance(errorCode, int))
        if errorCode == 1:
            log.debug("DHE: errorCode is 1")
            yield self.reply_ok(msg, response)
        else:
            log.debug("DHE: errorCode is NOT 1")
            yield self.reply_err(msg,
                                 "Error code %s, response: %s" % (errorCode,
                                                                  response))

    @defer.inlineCallbacks
    def op_execute_device(self, content, headers, msg):
        """
        Execute instrument commands relate to the instrument fronted by this
        Instrument Agent. These commands will likely be handled by the
        underlying driver.
        @param command An ordered list of lists where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval ACK message with response on success, ERR message with string
            indicating code and response message on fail
        @todo fix the return value hack
        """
        assert(isinstance(content, (tuple, list))), "Bad IA op_execute_device type"
        try:
            response = {}
            result = yield self.driver_client.execute(content)
            if result == {}:
                yield self.reply_err(msg, "Could not execute %s" % content)
                return
            else:
                response.update(result)

            assert(response != {})
            yield self.reply_ok(msg, response)
        except ReceivedError, re:
            yield self.reply_err(msg, "Failure, response is: %s" % re[1])

    @defer.inlineCallbacks
    def op_get_status(self, content, headers, msg):
        """
        Obtain the status of an instrument. This includes non-parameter
        and non-lifecycle state of the instrument.
        @param content A list of arguments to make up the status request
        @retval ACK message with response on success, ERR message on failure
        """
        assert(isinstance(content, (list, tuple)))
        try:
            response = yield self.driver_client.get_status(content)
            yield self.reply_ok(msg, response)
        except ReceivedError, re:
            yield self.reply_err(msg, re[1])

    @defer.inlineCallbacks
    def op_start_phrase(self, content, headers, msg):
        """
        Start a phrase. Must not already have a phrase started or ended
            pending application.
        @param timeout Duration of time before the phrase times out
        @retval Success with a phrase ID or failure with an explanation
        @todo Add check of end time to see old phrase self expired
        @todo Add timeouts argument
        """
        if (self.pending_phrase != None):
            yield self.reply_err(msg, "Phrase already started")
            return
        
        # Must be okay to start a phrase then. Add a placeholder with an
        # untyped phrase
            # *** Gotta figure out current time/time duration structures
            # Think something like:
            #self.pending_phrase = Phrase(current_time + timeout)
        self.pending_phrase = Phrase()
        yield self.reply_ok(msg, "Phrase created")
        
    @defer.inlineCallbacks
    def op_end_phrase(self, content, headers, msg):
        """
        End a phrase. Must have a phrase started and not ended
            pending application.
        @retval Success or failure with an explanation
        @todo Sort out observatory fetch
        @todo Implement locking/exclusion for fetch
        """
        if (self.pending_phrase == None):
            yield self.reply_err(msg, "No phrase started")
            return
        
        if (self.pending_phrase.is_complete()):
            yield self.reply_err(msg,
                            "Phrase already ended. Apply or cancel first.")
            return
        
        if (self.pending_phrase.is_expired()):
            self.pending_phrase = None
            yield self.reply_err(msg, "Phrase is expired")
            return

        # Get phrases get an immediate response
        # TODO Currently only for get_instrument calls
        if (isinstance(self.pending_phrase, GetPhrase)):
            phrase = self.pending_phrase.contents()
            assert(self.driver_client != None)
            response = {}
            for action in phrase:
                assert(action != None)
                if action.destination == self.pending_phrase.device:
                    response = yield self.driver_client.fetch_params(action.struct)
                    if response != {}:
                        yield self.reply_ok(msg, response)
                    else:
                        yield self.reply_err(msg, 'No values found')
                elif action.destination == self.pending_phrase.observatory:
                    #TODO: Get observatory values
                    pass
                else:
                    assert(False), "Really unknown action type"
            return
        
        # Must not be a get, so just mark the end
        self.pending_phrase.end()
        yield self.reply_ok(msg, "Phrase ended")        
           
    @defer.inlineCallbacks
    def op_cancel_phrase(self, content, headers, msg):
        """
        Cancel a phrase. Must have a phrase started.
        @retval Success or failure with an explanation
        """
        if (self.pending_phrase == None):
            yield self.reply_err(msg, 'No phrase active')
        else:
            self.pending_phrase = None
            yield self.reply_ok(msg, "Cancelled phrase")

    @defer.inlineCallbacks
    def op_apply_phrase(self, content, headers, msg):
        """
        Apply a phrase. Must have a phrase started.
        @retval Success or failure with an explanation
        @todo Figure out observatory sets
        @todo handle returns for each action
        """
        if (self.pending_phrase == None):
            yield self.reply_err(msg, "No phrase to apply")
            return    

        if (self.pending_phrase.is_expired()):
            self.pending_phrase = None
            yield self.reply_err(msg, "Phrase is expired")
            return
        
        phrase = self.pending_phrase.contents()
        
        # Dispatch phrase actions
        result = {}
        for action in phrase:
            if (isinstance(action, GetAction)):
                if (action.destination == Phrase.device):
                    result.append({action.struct:
                                   self._action_get_device(action)})
                if (action.destination == Phrase.observatory):
                    result.append({action.struct:
                                   self._action_get_observatory(action)})
            if (isinstance(action, SetAction)):
                if (action.destination == Phrase.device):
                    result.append({action.struct:
                                   self._action_set_device(action)})
                if (action.destination == Phrase.observatory):
                    result.append({action.struct:
                                   self._action_set_observatory(action)})
            if (isinstance(action, ExecuteAction)):
                if (action.destination == Phrase.device):
                    result.append({action.struct:
                                   self._action_execute_device(action)})
                if (action.destination == Phrase.observatory):
                    result.append({action.struct:
                                   self._action_execute_observatory(action)})
        
        yield self.reply_ok(msg, result)
                
    @defer.inlineCallbacks
    def op_publish(self, content, headers, msg):
        """
        Collect data from a subprocess (usually the driver) to publish to the
        correct topic, specific to the hardware device, not the agent.
        @param content A dict including: a Type string of "StateChange",
          "ConfigChange", "Error", or "Data", and a Value string with the
          data or message that is to be published. Must also have "Transducer"
          to specify the transducer doing the chagne.
        """
        assert isinstance(content, dict), "InstrumentAgent op_publish argument error"
        log.debug("Agent is publishing with sender: %s, child_procs: %s, content: %s",
                  headers["sender-name"], self.child_procs, content)
        if (self._is_child_process(headers["sender-name"])):
            if (content["Type"] == publish_msg_type["Data"]):
                yield self.pubsub_client.publish(self,
                            self.output_topics[content["Transducer"]].reference(),
                            content["Value"])
            elif ((content["Type"] == publish_msg_type["Error"])
                or (content["Value"] == "ConfigChange")):
                yield self.pubsub_client.publish(self,
                            self.event_topics[content["Transducer"]].reference(),
                            content["Value"])
            elif (content["Type"] == publish_msg_type["StateChange"]):
                yield self.pubsub_client.publish(self,
                            self.state_topics[content["Transducer"]].reference(),
                            content["Value"])
        else:
            # Really should be handled better...what if there isnt a reply
            # expected?
            yield self.reply_err(msg,
                                 "publish invoked from non-child process")
        # return something...like maybe result?
    
    @defer.inlineCallbacks
    def _self_publish(self, type, value):
        """
        Publish a message from the instrument agent to one of the agent
        pubsub topics. Possibly an event or a state change. Probably not data
        @param type The type of information to publish (should be "Error",
            "StateChange", "ConfigChange", "Event")
        @todo Actually write a test case for this!
        """
        assert ((type == publish_msg_type["Error"]) or \
            (type == publish_msg_type["Event"]) or \
        (type == publish_msg_type["StateChange"]) or \
        (type == publish_msg_type["ConfigChange"])), "Bad IA publish type"
        
        if (type == publish_msg_type["Error"]) or \
            (type == publish_msg_type["Event"]) or \
            (type == publish_msg_type["ConfigChange"]):
                yield self.pubsub_client.publish(self.sup,
                            self.event_topics["Agent"].reference(),value)
        if (type == publish_msg_type["StateChange"]):
                yield self.pubsub_client.publish(self.sup,
                            self.state_topics["Agent"].reference(),value)
    
    def _is_child_process(self, name):
        """
        Determine if a process with the given name is a child process
        @param name The name to test for subprocess-ness
        @retval True if the name matches a child process name, False otherwise
        """
        log.debug("__is_child_process looking for process '%s' in %s",
                  name, self.child_procs)
        found = False
        for proc in self.child_procs:
            if proc.proc_name == name:
                found = True
                break
        return found
        
    def _add_to_phrase(self, action):
        """
        Add the following action to a currently running phrase.
        @param action The valid structure for an action
        """
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
    def get_observatory(self, paramList):
        """
        Obtain a list of parameter names from the instrument, decode some
        values as needed.
        @param paramList A list of the values to fetch
        @retval A dict of the names and values requested
        """
        assert(isinstance(paramList, list))
        (content, headers, message) = yield self.rpc_send('get_observatory',
                                                          paramList)
        assert(isinstance(content, dict))
        for key in content.keys():
            if (key == ci_param_list['DataTopics']) or \
                (key == ci_param_list['EventTopics']) or \
                       (key == ci_param_list['StateTopics']):
                for entry in content[key].keys():
                    content[key][entry] = DataObject.decode(content[key][entry])
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
        log.debug("DHE: IAC in op_disconnect!")
        assert(isinstance(argList, list))
        (content, headers, message) = yield self.rpc_send('disconnect',
                                                              argList)
        assert(isinstance(content, dict))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def execute_device(self, command):
        """
        Execute the instrument commands in the order of the list.
        Processing will cease when a command fails, but will not roll back.
        For instrument calls, use executeInstrument()
        @see executeInstrument()
        @param command A list where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval Dictionary of responses to each execute command
        @todo Alter semantics of this call as needed...maybe a list?
        @todo Add exceptions as needed
        """
        assert(isinstance(command, list))
        (content, headers, message) = yield self.rpc_send('execute_device',
                                                          command)
        log.debug("message = " + str(message))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def execute_observatory(self, command):
        """
        Execute the instrument commands in the order of the list.
        Processing will cease when a command fails, but will not roll back.
        For instrument calls, use execute_device()
        @see execute_device()
        @param command A list where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval Dictionary of responses to each execute command
        @todo Alter semantics of this call as needed...maybe a command list?
        @todo Add exceptions as needed
        """
        assert(isinstance(command, list))
        (content, headers, message) = yield self.rpc_send('execute_observatory',
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
    def start_phrase(self, timeout=0):
        """
        Start a phrase. Must not already have a phrase started or ended
            pending application.
        @param timeout An optional timeout in time duration format
        @retval Success with a phrase ID or failure with an explanation
        """
        (content, headers, message) = yield self.rpc_send('start_phrase',
                                                          timeout)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def end_phrase(self):
        """
        End a phrase. Must have a phrase started and not ended
            pending application.
        @retval Success or failure with an explanation
        """
        (content, headers, message) = yield self.rpc_send('end_phrase', None)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def cancel_phrase(self):
        """
        Cancel a phrase. Must have a phrase started.
        @retval Success or failure with an explanation
        """
        (content, headers, message) = yield self.rpc_send('cancel_phrase', None)
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
    def register_resource(self, instrument_id):
        """
        Register the resource. Since this is a subclass, make the appropriate
        resource description for the registry and pass that into the
        registration call.
        """
        ia_instance = InstrumentAgentResourceInstance()
        ci_params = yield self.get_observatory([driver_address])
        ia_instance.driver_process_id = ci_params[driver_address]
        ia_instance.instrument_ref = ResourceReference(
            RegistryIdentity=instrument_id, RegistryBranch='master')
        result = yield ResourceAgentClient.register_resource(self,
                                                             ia_instance)
        defer.returnValue(result)
