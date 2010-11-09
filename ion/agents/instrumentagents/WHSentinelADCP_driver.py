#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/WHSentinelADCP_driver.py
@author Bill Bollenbacher
@brief Driver code for Teledyne RDI Workhorse Sentinel ADCP
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor

"""
from ion.data.dataobject import LCStates as LCS
from ion.core.process.service_process import ServiceProcess
"""

from instrument_hsm import InstrumentHsm

from ion.agents.instrumentagents.instrument_connection import InstrumentConnection
from twisted.internet.protocol import ClientCreator

from ion.core.process.process import Process
from ion.data.dataobject import ResourceReference
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject
from ion.services.dm.distribution.pubsub_service import DataPubsubClient

from ion.agents.instrumentagents.instrument_agent import InstrumentDriver, InstrumentAgentClient
from ion.agents.instrumentagents.instrument_agent import InstrumentDriverClient
from ion.agents.instrumentagents.WHSentinelADCP_constants import instrument_commands
from ion.agents.instrumentagents.WHSentinelADCP_constants import instrument_prompts

import ion.util.procutils as pu

from ion.core.process.process import ProcessFactory

import time
from socket import *
import sys
from threading import Timer

RESPONSE_TIMEOUT = 2   # 2 second timeout for response from instrument

class WHSentinelADCP_instCommandXlator():
    commands = {
       ' ' : ' '
    }

    def translate(self, command):
        if command in self.commands:
            return(self.commands[command])
        else:
            return(command)

class WHSentinelADCPInstrumentDriver(InstrumentDriver):
    """
    Maybe some day these values are looked up from a registry of common
    controlled vocabulary
    """

    def __init__(self, *args, **kwargs):
        self.connected = False
        self.instrument = None
        self.command = None
        self.setConnected(False)
        self.setTopicDefined(False)
        self.publish_to = None
        self.dataQueue = ""
        self.cmdQueue = []
        self.proto = None
        self.TimeOut = None
        NoResponseIsError = False
        
        self.instCmdXlator = WHSentinelADCP_instCommandXlator()
        
        self.hsm = InstrumentHsm()
        
        # --------------------------------------------------------------------
        #             name                               parent's
        #              of              event             event
        #             state            handler           handler
        # --------------------------------------------------------------------
        self.hsm.addState ( "idle",                 self.idle,                  None)
        self.hsm.addState ( "stateConfigured",      self.stateConfigured,       self.idle)
        self.hsm.addState ( "stateDisconnecting",   self.stateDisconnecting,    self.stateConfigured)
        self.hsm.addState ( "stateDisconnected",    self.stateDisconnected,     self.stateConfigured)
        self.hsm.addState ( "stateConnecting",      self.stateConnecting,       self.stateConfigured)
        self.hsm.addState ( "stateConnected",       self.stateConnected,        self.stateConfigured)
        self.hsm.addState ( "statePrompted",        self.statePrompted,         self.stateConnected)
        self.hsm.addState ( "stateDisconnecting",   self.stateDisconnecting,    self.stateConfigured)
    
        """
        A translation dictionary to translate from the commands being sent
        from the agent to the actual command understood by the instrument.
        """
        self.ParmCommands = {
            "baudrate" : "Baud"
            }

        self.__instrument_parameters = {
            "baudrate": 9600
        }

        InstrumentDriver.__init__(self, *args, **kwargs)

    # Change this to stateIdle
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
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.debug("idle state: ignoring event %s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
            log.debug("idle state: ignoring event %s;" %(caller.tEvt['sType']))
            data = self.dequeueData()            
            log.debug("stateIdle received %s." % (data))
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
        elif caller.tEvt['sType'] == "eventDisconnectComplete":
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
            # A command has been received from the agent
            # Move to stateConnecting to try to connect
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
        elif caller.tEvt['sType'] == "eventConnectionComplete":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            # move to stateConnected
            caller.stateTran(self.stateConnected)
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateDisconnecting state
            #
            caller.stateTran(self.stateDisconnecting)
            return 0
        # Don't think I should get this here...candidate for deletion
        elif caller.tEvt['sType'] == "eventPromptReceived":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
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
            if self.cmdPending():
                self.sendBreak()
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventResponseTimeout":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            self.ProcessWakeupResponseTimeout()
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            if self.TimeOut == None:
                # We got a command from the agent; need to get the prompt
                # before sending
                self.sendBreak()
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            if self.TimeOut != None:
                self.TimeOut.cancel()
                self.TimeOut = None
            self.ProcessRcvdData()
            return 0
        elif caller.tEvt['sType'] == "eventPromptReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            if self.TimeOut != None:
                self.TimeOut.cancel()
                self.TimeOut = None
            caller.stateTran(self.statePrompted)
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateDisconnecting state
            #
            caller.stateTran(self.stateDisconnecting)
            return 0
        return caller.tEvt['sType']

    def statePrompted(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In statePrompted state")
        if caller.tEvt['sType'] == "init":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            """
            @TODO Need a queue of commands from which to pull commands
            """
            if self.cmdPending():
                self.sendCmd(self.cmdQueue[0])
                self.NoResponseIsError = True
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventResponseTimeout":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            self.ProcessCmdResponseTimeout()
            caller.stateTran(self.stateConnected)
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            if self.cmdPending():
                self.sendCmd(self.cmdQueue[0])
            else:
                log.error("statePrompted: No command to send!")
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            self.ProcessRcvdData()
            return 0
        elif caller.tEvt['sType'] == "eventPromptReceived":
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            log.info("statePrompted-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateDisconnecting state
            #
            caller.stateTran(self.stateDisconnecting)
            return 0
        return caller.tEvt['sType']

    def stateDisconnecting(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateDisconnecting state")
        if caller.tEvt['sType'] == "init":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            if (self.proto):
                log.debug("disconnecting from instrument")
                self.proto.transport.loseConnection()
            else:
                log.debug("no proto instance: cannot disconnect")
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectComplete":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnected state
            #
            caller.stateTran(self.stateDisconnected)
            return 0
        return caller.tEvt['sType']

    @defer.inlineCallbacks
    def plc_init(self):
        log.debug("WHSentinelADCPInstrumentDriver.plc_init: spawn_args: %s" %str(self.spawn_args))
        self.instrument_id = self.spawn_args.get('instrument-id', '123')
        self.instrument_ipaddr = self.spawn_args.get('ipaddr', "localhost")
        self.instrument_ipport = self.spawn_args.get('ipport', 9000)
        self.instrument_ipportCmd = self.spawn_args.get('ipportCmd', 967)

        # DHE Testing HSM
        log.debug("!!!!!!!!!!!!!!!!!! Calling onStart!")
        self.hsm.onStart(self.idle)

        yield self._configure_driver(self.spawn_args)

        log.info("INIT DRIVER for instrument ID=%s, ipaddr=%s, port=%s, publish-to=%s" % (
            self.instrument_id, self.instrument_ipaddr, self.instrument_ipport, self.publish_to))

        self.iaclient = InstrumentAgentClient(proc=self, target=self.proc_supid)

        log.debug("Instrument driver initialized")

        log.debug("!!!!!!!!!!! Sending configured event");
        self.hsm.onEvent('configured');

    @defer.inlineCallbacks
    def plc_terminate(self):
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
        
    def enqueueCmd(self, cmd):
        log.debug("enqueueCmd: enqueueing command: %s" %cmd)
        self.cmdQueue.append(cmd)

    def dequeueCmd(self):
        cmd = self.cmdQueue.pop()
        #log.debug("dequeueCmd: dequeueing command: %s" %cmd)
        return cmd

    def cmdPending(self):
        if len(self.cmdQueue) > 0:
            return True
        else:
            return False
        
    def TimeoutCallback(self):
        log.info("TimeoutCallback()")
        if self.TimeOut != None:
            self.TimeOut = None
            self.hsm.onEvent('eventResponseTimeout')
        
    def ProcessCmdResponseTimeout(self):
        log.error("No response from instrument for cammand \'%s\'" % self.cmdQueue[0])
        self.cmdQueue = []
        self.publish("No response from instrument for cammand \'%s\'".format(self.cmdQueue[0]), self.publish_to)

    def ProcessWakeupResponseTimeout(self):
        log.error("No response from instrument for wakeup")
        self.cmdQueue = []
        self.publish("No response from instrument for wakeup", self.publish_to)
      
    def sendCmd(self, cmd):
        log.debug("Sending Command: %s" %cmd)
        self.instrument.transport.write(cmd)
        self.TimeOut = Timer(RESPONSE_TIMEOUT, self.TimeoutCallback)
        self.TimeOut.start()
        
        
    def sendBreak(self):
        if self.instrument_ipaddr == "localhost" or self.instrument_ipaddr == "127.0.0.1":
            # localhost(127.0.0.1) implies the use of the instrument simulator so just send a simple CR/LF
            log.info("Sending CR/LF to simulator")
            self.instrument.transport.write(instrument_prompts.PROMPT_INST)
        else:
            log.info("Sending break to instrument ipaddr: %s, ipport: %s" %(self.instrument_ipaddr, self.instrument_ipportCmd))      
            try:
                s = socket(AF_INET, SOCK_STREAM)    # create a TCP socket   
                s.settimeout(2)                     # set timeout to 2 seconds for reads
                s.connect((self.instrument_ipaddr, self.instrument_ipportCmd))
                s.send('\x21\x00')                  # start sending break
                data = s.recv(1024)                 # receive up to 1K bytes
                if data != '\x21OK':
                    raise RuntimeError('OK response not received')
                log.debug("Rcvd OK response for 'start break' cmd")
                time.sleep (1)
                s.send('\x22\x00')                  # stop sending break
                data = s.recv(1024)                 # receive up to 1K bytes
                if data != '\x22OK':
                    raise RuntimeError('OK response not received')
                log.debug("Rcvd OK response for 'stop break' cmd")
            except:
                log.error("Send break failed: %s" % sys.exc_info()[1])
                return
        self.TimeOut = Timer(RESPONSE_TIMEOUT, self.TimeoutCallback)
        self.TimeOut.start()

                
    def ProcessRcvdData(self):
        # for now, publish a complete line at a time
        if instrument_prompts.PROMPT_INST in self.dataQueue:
            partition = self.dataQueue.partition(instrument_prompts.PROMPT_INST)
            self.dataQueue = partition[2]
            if self.cmdQueue[0] in partition[0]:
                log.debug("Command \'%s\' received" % self.cmdQueue[0])
                self.cmdQueue.pop()
                if self.TimeOut != None:
                    self.TimeOut.cancel()
                    self.TimeOut = None
            # send this up to the agent to publish.
            log.info("Calling publish with \"%s\"" %partition[0])
            self.publish(partition[0], self.publish_to)
            

        
    @defer.inlineCallbacks
    def getConnected(self):
        """
        @brief A method to get connected to the instrument device server via
        a TCP/IP device server.  We need to come up with a more flexible way of
        doing this; like getting a connection object that abstracts the details
        of the protocol. Not sure how easy that would be with Twisted and
        Python.

        @retval None.
        """

        cc = ClientCreator(reactor, InstrumentConnection, self)
        log.info("Driver connecting to instrument ipaddr: %s, ipport: %s" %(self.instrument_ipaddr, self.instrument_ipport))
        self.proto = yield cc.connectTCP(self.instrument_ipaddr, int(self.instrument_ipport))
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

        #
        # Don't need this anymore
        #
        self.setConnected(True)
        
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

        self.hsm.onEvent('eventDisconnectComplete')

        #
        # Don't need this anymore
        #
        self.setConnected(False)

    def gotData(self, data):
        """
        @brief The instrument protocol object has received data from the
        instrument. 
        @param data
        @retval none
        """
        if instrument_prompts.INST_PROMPT in data:
            log.info("gotData(): prompt seen")
            self.hsm.onEvent('eventPromptReceived')
        DataAsHex = ""
        for i in range(len(data)):
            if len(DataAsHex) > 0:
                DataAsHex += ","
            DataAsHex += "{0:X}".format(ord(data[i]))
        log.debug("gotData() [%s] [%s]" % (data, DataAsHex))
        self.dataQueue += data
        self.hsm.onEvent('eventDataReceived')
        

    @defer.inlineCallbacks
    def publish(self, data, topic):
        """
        @brief Publish the given data to the given topic.
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
        self.hsm.onEvent('eventDisconnectReceived')
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

        assert(isinstance(content, dict))
        log.debug("op_set_params content: %s, keys: %s" %(str(content), str(content.keys)))

        error_msg = None
        for param in content.keys():
            if (param not in self.__instrument_parameters):
                # Getting rid of this
                #yield self.reply_err(msg, "Could not set %s" % param)
                error_msg = "Could not set " + str(param)
                log.error(error_msg)
                # NEED TO BREAK OUT HERE: don't send multiple responses
                break;
            else:
                self.__instrument_parameters[param] = content[param]
                if param in self.ParmCommands:
                    log.info("current param is: %s" %str(param))
                    command = self.ParmCommands[param] + "=" + str(content[param])
                    """
                    Send the command received event.  This should kick off the
                    appropriate sequence of events to get the command sent.
                    """
                    self.enqueueCmd(command)
                    self.hsm.onEvent('eventCommandReceived')
                else:
                    error_msg = str(param) + " is not a settable parameter"
                    #log.error("%s is not a settable parameter" % str(param))
                    log.error(error_msg)
                    
        if error_msg:
            yield self.reply_err(msg, error_msg)
        else:            
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

        if ((content == ()) or (content == [])):
            yield self.reply_err(msg, "Empty command")
            return
        agentCommands = []
        for command_set in content:
            command = command_set[0]
            value = command_set[1]
            if command not in instrument_commands:
                log.error("Invalid Command: %s" %command)
                yield self.reply_err(msg, "Invalid Command")
            else:
                log.debug("op_execute translating command: %s" % command)
                instCommand = self.instCmdXlator.translate(command)
                instCommand += value
                log.debug("op_execute would send command: %s to instrument" % instCommand)
                instCommand += instrument_prompts.PROMPT_INST

                self.enqueueCmd(instCommand)

                """
                Send the command received event.  This should kick off the
                appropriate sequence of events to get the command sent.
                """
                self.hsm.onEvent('eventCommandReceived')

                agentCommands.append(command)
                yield self.reply_ok(msg, agentCommands)


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
        log.info("!!!!! _configure_driver! params: %s" %str(params))
        
        if 'ipaddr' in params:
            self.instrument_ipaddr = params['ipaddr']
        else:
            log.debug("%%%%%%%% No ipaddr in params: defaulting to: %s" %self.instrument_ipaddr)
            
        if 'ipport' in params:
            self.instrument_ipport = params['ipport']
        else:
            log.debug("%%%%%%%% No ipport in params: defaulting to: %s" %self.instrument_ipport)
 
        if 'ipportCmd' in params:
            self.instrument_ipportCmd = params['ipportCmd']
        else:
            log.debug("%%%%%%%% No ipportCmd in params: defaulting to: %s" %self.instrument_ipportCmd)
            
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

class WHSentinelADCPInstrumentDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """


# Spawn of the process using the module name
factory = ProcessFactory(WHSentinelADCPInstrumentDriver)
