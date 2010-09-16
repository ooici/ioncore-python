#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE49_instrument_driver.py
@author Steve Foley
@author Dave Everett
@brief Driver code for SeaBird SBE-49 CTD
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor

"""
from ion.data.dataobject import LCStates as LCS
from ion.services.base_service import BaseService
"""

# DHE: testing the miros HSM
import miros

from twisted.internet.protocol import Protocol, ClientFactory, ClientCreator

from ion.core.base_process import BaseProcess
from ion.data.dataobject import ResourceReference
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject
from ion.services.dm.distribution.pubsub_service import DataPubsubClient

from ion.agents.instrumentagents.instrument_agent import InstrumentDriver, InstrumentAgentClient
from ion.agents.instrumentagents.instrument_agent import InstrumentDriverClient
from ion.agents.instrumentagents.SBE49_constants import instrument_commands

import ion.util.procutils as pu

from ion.core.base_process import ProtocolFactory

class InstrumentClient(Protocol):
    """
    The InstrumentClient class; inherits from Protocol.  Override dataReceived
    and call the factory data_received() method to get the data to the agent.
    """
    def __init__(self, parent):
        self.parent = parent

    def connectionMade(self):
        log.debug("connectionMade, calling gotConnected().")
        self.parent.gotConnected(self)
        
    def connectionLost(self, reason):
        log.debug("connectionLost, calling gotDisconnected()")
        self.parent.gotDisconnected(self)

    def dataReceived(self, data):
        """
        Filter the data; the instrument will send the
        prompt, which we don't care about, I'm assuming.  We might need a sort
        of state machine or something; for instance, the agent sends a getStatus
        command, we need to know that we're expecting a status message.
        """
        log.debug("dataReceived!")
        if data == 'S>':
            log.debug("received Seabird prompt.")
            #self.factory.prompt_received(self)
            self.parent.gotPrompt(self)
        elif data == '?CMD':
            log.info("Seabird doesn't understand command.")
        else:
            log.debug("dataReceived()!")
            #self.factory.data_received(data)
            self.parent.gotData(data)


class SBE49InstrumentDriver(InstrumentDriver):
    """
    Maybe some day these values are looked up from a registry of common
        controlled vocabulary
    """

    def __init__(self, receiver=None, spawnArgs=None, **kwargs):
        self.connected = False
        self.instrument = None
        self.command = None
        self.setTopicDefined(False)
        self.publish_to = None

        #
        # DHE: Testing miros FSM.
        #
        self.hsm = miros.Hsm()

        # --------------------------------------------------------------------
        #             name                               parent's
        #              of              event             event
        #             state            handler           handler
        # --------------------------------------------------------------------
        self.hsm.addState ( "idle",           self.idle,               None)
        self.hsm.addState ( "stateConfigured",  self.stateConfigured,     self.idle)
        self.hsm.addState ( "stateDisconnected",  self.stateDisconnected,     self.stateConfigured)
        self.hsm.addState ( "stateConnecting",  self.stateConnecting,     self.stateConfigured)
        self.hsm.addState ( "stateConnected",  self.stateConnected,     self.stateConfigured)
    
        """
        A translation dictionary to translate from the commands being sent
        from the agent to the actual command understood by the instrument.
        """
        self.sbeParmCommands = {
            "baudrate" : "Baud",
            "outputformat" : "outputformat"
        }

        self.__instrument_parameters = {
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

        InstrumentDriver.__init__(self, receiver, spawnArgs, **kwargs)

    def top(self, caller):
        log.debug("!!!!!!!!!!!!!!! In top state")
        if caller.tEvt['sType'] == "init":
            # display event
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            # transition to state idle
            caller.stateStart(self.idle)
            # returning a 0 indicates event was handled
            return 0
        elif caller.tEvt['sType'] == "entry":
            # display event, do nothing 
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            self.tEvt['nFoo'] = 0
            return 0
        return caller.tEvt['sType']

    def idle(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In idle state")
        if caller.tEvt['sType'] == "init":
            # display event
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "entry":
            # display event, do nothing 
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            self.tEvt['nFoo'] = 0
            return 0
        elif caller.tEvt['sType'] == "configured":
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            log.info("!!!!!! transitioning to stateConfigured! idle-%s;" %(caller.tEvt['sType']))
            self.hsm.stateTran(self.stateConfigured)
            return 0
        return caller.tEvt['sType']

    def stateConfigured(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConfigured state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            caller.stateStart(self.stateDisconnected)
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            return 0
        return caller.tEvt['sType']

    def stateDisconnected(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateDisconnected state")
        if caller.tEvt['sType'] == "init":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnecting state
            #
            caller.stateTran(self.stateConnecting)
            return 0
        return caller.tEvt['sType']

    def stateConnecting(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConnecting state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            self.getConnected()
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            return 0
            return 0
        elif caller.tEvt['sType'] == "eventConnectionComplete":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnected state
            #
            caller.stateTran(self.stateConnected)
            return 0
        return caller.tEvt['sType']

    def stateConnected(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConnected state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            """
            @todo Need a queue of commands from which to pull commands
            """
            # if command pending
            if self.command:
               self.instrument.transport.write(self.command)
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            # if command pending
            if self.command:
               self.instrument.transport.write(self.command)
            return 0
        return caller.tEvt['sType']

    @defer.inlineCallbacks
    def plc_init(self):
        self.instrument_id = self.spawn_args.get('instrument-id', '123')
        self.instrument_port = self.spawn_args.get('port', 9000)

        # DHE Testing HSM
        log.debug("!!!!!!!!!!!!!!!!!! Calling onStart!")
        self.hsm.onStart(self.idle)

        yield self._configure_driver(self.spawn_args)

        log.info("INIT DRIVER for instrument ID=%s, port=%s, publish-to=%s" % (
            self.instrument_id, self.instrument_port, self.publish_to))

        self.iaclient = InstrumentAgentClient(proc=self, target=self.proc_supid)

        log.debug("Instrument driver initialized")

        log.debug("!!!!!!!!!!! Sending configured event");
        self.hsm.onEvent('configured');

    @defer.inlineCallbacks
    def plc_shutdown(self):
        yield self.op_disconnect(None, None, None)

    def isConnected(self):
        return self.connected

    def setConnected(self, value):
        self.connected = value;

    def isTopicDefined(self):
        return self.topicDefined

    def setTopicDefined(self, value):
        log.info("*******setting topicDefined to %s:" %str(value))
        self.topicDefined = value;

    def setAgentService(self, agent):
        self.agent = agent

    @defer.inlineCallbacks
    def getConnected(self):
        """
        @brief A method to get connected to the instrument device server.  Right
        now this assumes the device is connected via a TCP/IP device server.
        We probably need to come up with a more flexible way of doing this; like
        getting a connection object that abstracts the details of the protocol.
        Not sure how easy that would be with Twisted and Python.

        Gets a deferred object passes it to the InstrumentClientFactory, which
        uses it to acess callbacks.  Was trying to use this to make the
        connection process more managable.  Not sure if that's the case or  not
        yet.
        @retval The deferred object.
        """

        # Now thinking I might try clientcreator since this will only be a
        # single connection.
        cc = ClientCreator(reactor, InstrumentClient, self)
        self.proto = yield cc.connectTCP("localhost", self.instrument_port)
        log.info("Driver connected to instrument")

    def gotConnected(self, instrument):
        """
        @brief This method is called when a connection has been made to the
        instrument device server.  The instrument protocol object is passed
        as a parameter, and a reference to it is saved.  Call setConnected
        with True argument.
        @param reference to instrument protocol object.
        @retval none
        """
        log.debug("gotConnected!!!")

        self.instrument = instrument
        self.setConnected(True)
        
        # NEW NEW
        self.hsm.onEvent('eventConnectionComplete')


    def gotDisconnected(self, instrument):
        """
        @brief This method is called when a connection to the instrument 
        device server has been lost.  The instrument protocol object is passed
        as a parameter.  Call setConnected with False argument.
        @param reference to instrument protocol object.
        @retval none
        """
        log.debug("gotDisconnected!!!")

        self.instrument = instrument
        self.setConnected(False)

    def gotData(self, data):
        """
        @brief The instrument protocol object has received data from the
        instrument.  It should already be sanitized and ready for consumption;
        publish the data.
        @param data
        @retval none
        """
        # send this up to the agent to publish.
        log.debug("gotData() %s Calling publish." % (data))
        self.publish(data, self.publish_to)

    def gotPrompt(self, instrument):
        """
        This needs to be the general receive routine for the instrument driver
        """
        log.debug("gotPrompt()")
        #self.instrument = instrument
        #self.setConnected(True)

        """
        Do we need some sort of state machine so we'll know what data we're
        supposed to send here?  Right now it's working without...
        """

    @defer.inlineCallbacks
    def publish(self, data, topic):
        """
        @Brief Publish the given data to the given topic.
        @param data The data to publish
        @param topic The topic to which to publish.  Currently this is not the
        topic as defined by pubsub.
        @retval none
        """
        log.debug("publish()")
        if self.isTopicDefined() == True:

            # Create and send a data message
            result = yield self.dpsc.publish(self, self.topic.reference(), data)
            if result:
                log.info('Published Message')
            else:
                log.info('Failed to Published Message')
        else:
            log.info("NOT READY TO PUBLISH")


    @defer.inlineCallbacks
    def op_initialize(self, content, headers, msg):
        log.debug('In driver initialize')

        yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_disconnect(self, content, headers, msg):
        log.debug("in Instrument Driver op_disconnect!")
        if (self.isConnected()):
            log.debug("disconnecting from instrument")
            #self.connector.disconnect()
            self.proto.transport.loseConnection()
            self.setConnected(False)
        if msg:
            yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_fetch_params(self, content, headers, msg):
        """
        Operate in instrument protocol to get parameter
        @todo Write the code to interface this with something
        """
        assert(isinstance(content, (list, tuple)))
        result = {}
        for param in content:
            result[param] = self.__instrument_parameters[param]
        yield self.reply_ok(msg, result)

    @defer.inlineCallbacks
    def op_set_params(self, content, headers, msg):
        """
        Operate in instrument protocol to set a parameter. Current semantics
        are that, if there is a problem, fail as soon as possible. This may
        leave partial settings made in the device.
        @param content A dict of all the parameters and values to set
        @todo Make this an all-or-nothing and/or rollback-able transaction
            list?
        """

        """
        This connection stuff could be abstracted into a communications object.
        """
        if self.isConnected() == False:
            log.debug("yielding for connect")
            yield self.getConnected()
            log.debug("connect returned")

        assert(isinstance(content, dict))
        log.debug("op_set_params content: %s, keys: %s" %(str(content), str(content.keys)))
        
        for param in content.keys():
            if (param not in self.__instrument_parameters):
                yield self.reply_err(msg, "Could not set %s" % param)
            else:
                self.__instrument_parameters[param] = content[param]
                if param in self.sbeParmCommands:
                    if self.isConnected():
                        log.info("current param is: %s" %str(param))
                        command = self.sbeParmCommands[param] + "=" + str(content[param])
                        log.debug("op_set_params sending %s to instrument"  %str(command))
                        self.instrument.transport.write(command)
                else:
                    log.error("%s is not a settable parameter" % str(param))
        yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute the given command structure (first element command, rest
        of the elements are arguments)
        @todo actually do something
        """
        assert(isinstance(content, (tuple, list)))

        log.debug("op_execute content: %s" %str(content))
        """
        This connection stuff could be abstracted into a communications object.
        """
        if self.isConnected() == False:
            log.info("would be trying to connect this is a test")
            #log.info("yielding for connect")
            #yield self.getConnected()
            log.info("connect returned")
            # DHE NOTE TO SELF: not using the addCallback anymore, but it might 
            # be a good way to implement a state machine.
            #d.addCallback(self.gotConnected);
            #d.addCallback(self.gotPrompt);

        if ((content == ()) or (content == [])):
            yield self.reply_err(msg, "Empty command")
            return
        commands = []
        for command_set in content:
            command = command_set[0]
            if command not in instrument_commands:
                log.error("Invalid Command")
                yield self.reply_err(msg, "Invalid Command")
            else:
                log.debug("op_execute sending command: %s to instrument" % command)
                self.command = command
         
                """
                Send the command received event.  This should kick off the
                appropriate sequence of events to get the command sent.
                """
                self.hsm.onEvent('eventCommandReceived')

                """
                Currently sending the command from right here.  We SHOULD be
                connected at this point.
                """
                if self.isConnected():
                    log.info("!!!!! WOULD BE SENDING COMMAND HERE")
                    #self.instrument.transport.write(self.command)
                else:
                    log.error("op_execute: instrument not connected.")
                commands.append(command)
        yield self.reply_ok(msg, commands)


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
        
        log.info("!!!!!! op_configure_driver!")
        yield self._configure_driver(content)
        # Do something here, then adjust test case
        yield self.reply_ok(msg, content)

    @defer.inlineCallbacks
    def _configure_driver(self, params):
        """
        Configures driver params either on startup or on command
        """
        log.info("!!!!! _configure_driver!")
        
        if 'publish-to' in params:
            self.publish_to = params['publish-to']
            log.debug("Configured publish-to=" + self.publish_to)
            self.setTopicDefined(True)
            self.dpsc = DataPubsubClient(proc=self)
            self.topic = ResourceReference(RegistryIdentity=self.publish_to, RegistryBranch='master')
            self.publisher = PublisherResource.create('Test Publisher', self, self.topic, 'DataObject')
            self.publisher = yield self.dpsc.define_publisher(self.publisher)
        else:
            log.debug("%%%%%%%% No publish-to in params")

class SBE49InstrumentDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """


# Spawn of the process using the module name
factory = ProtocolFactory(SBE49InstrumentDriver)
