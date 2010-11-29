#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/WHSentinelADCP_driver.py
@author Bill Bollenbacher
@brief Driver code for Teledyne RDI Workhorse Sentinel ADCP
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer, reactor

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

import logging
import time
from socket import *
import sys
from threading import Timer, BoundedSemaphore
from collections import deque
from twisted.internet.protocol import Protocol
                       
CMD_RESPONSE_TIMEOUT = 3     # 2 second timeout for command response from instrument
BREAK_RESPONSE_TIMEOUT = 4   # 4 second timeout for break response from instrument

class CmdPort(Protocol):
    
    def __init__(self, parent):
        self.parent = parent
        log.info("CmdPort __init__")

    def connectionMade(self):
        """
        @brief A client has made a connection:
        """
        log.info("CmdPort connectionMade")
        self.parent.gotCmdConnected(self)

    def dataReceived(self, data):
        if log.getEffectiveLevel() == logging.DEBUG:
            DataAsHex = ""
            for i in range(len(data)):
                if len(DataAsHex) > 0:
                    DataAsHex += ","
                DataAsHex += "{0:X}".format(ord(data[i]))
            log.debug("CmdPort dataReceived [%s] [%s]" % (data, DataAsHex))
        else:
            log.info("CmdPort dataReceived [%s]" % data)
        self.parent.gotCmdData(data)
            
    def connectionLost(self, reason):
        #log.debug("CmdPort connectionLost - %s" % reason)
        log.debug("CmdPort connectionLost")

        
class WHSentinelADCP_instCommandXlator():
    commands = {
       'start' : ['cr1', 'cf11211', 'wp2', 'te00000300', 'tp000100', 'ck', 'cs'],
       'stop' : ['break', 'cr1', 'cz']
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
        self.SendingBreak = False
        self.instrument = None
        self.CmdInstrument = None
        self.command = None
        self.setTopicDefined(False)
        self.publish_to = None
        self.dataQueue = ""
        self.cmdQueue = deque([])
        self.proto = None
        self.TimeOut = None
        NoResponseIsError = False
        
        self.instCmdXlator = WHSentinelADCP_instCommandXlator()
        
        self.hsm = InstrumentHsm()
        
        # ------------------------------------------------------------------------------------
        #                   name                                                parent's
        #                   of                      event                       event
        #                   state                   handler                     handler
        # ------------------------------------------------------------------------------------
        self.hsm.addState ( "stateBase",            self.stateBase,             None)
        self.hsm.addState ( "stateUnconfigured",    self.stateUnconfigured,     self.stateBase)
        self.hsm.addState ( "stateConfigured",      self.stateConfigured,       self.stateBase)
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
            "baudrate" : "cb"
            }

        self.__instrument_parameters = {
            "baudrate": 9600,
            "parity": 1,
            "stopBits": 1
        }

        InstrumentDriver.__init__(self, *args, **kwargs)

    def stateBase(self, caller):
        log.debug("stateBase-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            self.tEvt['nFoo'] = 0
            return 0
        elif caller.tEvt['sType'] == "configured":
            self.hsm.stateTran(self.stateConfigured)
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            # TODO: Should these comands be flushed from the queue?
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            # since this event may occur at any time because 'process' calls
            # plc_terminate(), just ignore it if the state doesn't catch it
            log.info("stateBase - Explicitly ignoring event %s" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
            #TODO: Should this data be flushed from queue?
            return 0
        else:
            log.error("stateBase state: ##########********!!!!!!! UNHANDLED event %s !!!!!!!!!********###########" %(caller.tEvt['sType']))
            return 0
        return caller.tEvt['sType']

    def stateUnconfigured(self, caller):
        log.debug("stateUnconfigured-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            return 0
        elif caller.tEvt['sType'] == "eventConfigured":
            self.hsm.stateTran(self.stateConfigured)
            return 0
        return caller.tEvt['sType']

    def stateConfigured(self, caller):
        log.debug("stateConfigured-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            caller.stateStart(self.stateDisconnected)
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            return 0
        return caller.tEvt['sType']

    def stateDisconnected(self, caller):
        log.debug("stateDisconnected-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            # A command has been received from the agent
            # Move to stateConnecting to try to connect to instrumment
            caller.stateTran(self.stateConnecting)
            return 0
        return caller.tEvt['sType']

    def stateConnecting(self, caller):
        log.debug("stateConnecting-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            self.getConnected()
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            return 0
        elif caller.tEvt['sType'] == "eventConnectionFailed":
            caller.stateTran(self.stateDisconnected)
            return 0
        elif caller.tEvt['sType'] == "eventConnectionComplete":
            caller.stateTran(self.stateConnected)
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            caller.stateTran(self.stateDisconnecting)
            return 0
        return caller.tEvt['sType']

    def stateConnected(self, caller):
        log.debug("stateConnected-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            if self.cmdPending():
                self.sendBreak()
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            if self.TimeOut == None:
                # We got a command from the agent; need to get the prompt
                # before sending
                self.sendBreak()
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
            self.ProcessRcvdData()
            return 0
        elif caller.tEvt['sType'] == "eventPromptReceived":
            if self.TimeOut != None:
                self.TimeOut.cancel()
                self.TimeOut = None
            caller.stateTran(self.statePrompted)
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            caller.stateTran(self.stateDisconnecting)
            return 0
        elif caller.tEvt['sType'] == "eventResponseTimeout":
            self.ProcessWakeupResponseTimeout()
            return 0
        return caller.tEvt['sType']

    def statePrompted(self, caller):
        log.debug("statePrompted-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            if self.cmdPending():
                if (self.TimeOut == None) and (self.SendingBreak == False):
                    self.sendCmd(self.PeekCmd())
                # Only way to get here is from stateConnected after receiving a
                # prompt, so not getting a response is an error condition
                self.NoResponseIsError = True
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            return 0
        elif caller.tEvt['sType'] == "eventResponseTimeout":
            if self.NoResponseIsError == True:
                self.ProcessCmdResponseTimeout()
            # Goto connected state since command response failed
            caller.stateTran(self.stateConnected)
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            if self.cmdPending():
                if (self.TimeOut == None) and (self.SendingBreak == False):
                    self.sendCmd(self.PeekCmd())
            else:
                log.error("statePrompted: No command to send!")
            return 0
        elif caller.tEvt['sType'] == "eventDataReceived":
             self.ProcessRcvdData()
             return 0
        elif caller.tEvt['sType'] == "eventPromptReceived":
            log.info("statePrompted - Explicitly ignoring PromptReceived event")
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            caller.stateTran(self.stateDisconnecting)
            return 0
        return caller.tEvt['sType']

    def stateDisconnecting(self, caller):
        log.debug("stateDisconnecting-%s;" %(caller.tEvt['sType']))
        if caller.tEvt['sType'] == "init":
            if (self.proto):
                log.debug("disconnecting from instrument")
                self.proto.transport.loseConnection()
            else:
                log.debug("no proto instance: cannot disconnect")
            return 0
        elif caller.tEvt['sType'] == "entry":
            return 0
        elif caller.tEvt['sType'] == "exit":
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectComplete":
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

        log.debug("!!!!!!!!!!!!!!!!!! Calling onStart!")
        self.hsm.onStart(self.stateUnconfigured)

        yield self._configure_driver(self.spawn_args)

        log.info("INIT DRIVER for instrument ID=%s, ipaddr=%s, port=%s, publish-to=%s" % (
            self.instrument_id, self.instrument_ipaddr, self.instrument_ipport, self.publish_to))

        self.iaclient = InstrumentAgentClient(proc=self, target=self.proc_supid)

        log.debug("Instrument driver initialized")

        log.debug("!!!!!!!!!!! Sending configured event");
        self.hsm.sendEvent('eventConfigured');


    @defer.inlineCallbacks
    def plc_terminate(self):
        if self.TimeOut != None:
            self.TimeOut.cancel()
            self.TimeOut = None
        self.hsm.sendEvent('eventDisconnectReceived')


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
        if len(self.cmdQueue) > 0:     
            cmd = self.cmdQueue.popleft()
            log.info("dequeueCmd: dequeueing command: %s" %cmd)
            return cmd


    def PeekCmd(self):
        if len(self.cmdQueue) > 0:     
            return self.cmdQueue[0]
        else:
            return ""


    def cmdPending(self):
        if len(self.cmdQueue) > 0:
            return True
        else:
            return False
        

    def TimeoutCallback(self, Reason):
        if Reason == "C":
            log.info("TimeoutCallback() for Command")
        else:
            log.info("TimeoutCallback() for Break")
        if self.TimeOut != None:
            self.TimeOut = None
            self.hsm.sendEvent('eventResponseTimeout')
        

    def ProcessCmdResponseTimeout(self):
        log.error("No response from instrument for cammand \'%s\'" % self.cmdQueue[0])
        self.publish("No response from instrument for cammand \'%s\'".format(self.cmdQueue[0]), self.publish_to)
        self.cmdQueue = []
        self.NoResponseIsError = False
 

    def ProcessWakeupResponseTimeout(self):
        log.error("No response from instrument for wakeup")
        self.publish("No response from instrument for wakeup", self.publish_to)
        self.cmdQueue = []
      

    def sendCmd(self, cmd):
        log.info("Sending Command: %s" %cmd)
        if cmd == "break":
            self.sendBreak()
        else:
            self.instrument.transport.write(cmd + instrument_prompts.DRIVER_LINE_TERMINATOR)
            self.TimeOut = Timer(CMD_RESPONSE_TIMEOUT, self.TimeoutCallback, "C")
            self.TimeOut.start()
        
        
    @defer.inlineCallbacks
    def sendBreak(self):
        if self.SendingBreak == True:
            log.debug("already sending break")
            return
        else:
            self.SendingBreak = True
        Cmdcc = ClientCreator(reactor, CmdPort, self)
        log.info("Driver connecting to instrument command port ipaddr: %s, ipportCmd: %s" %(self.instrument_ipaddr, self.instrument_ipportCmd))
        self.Cmdproto = yield Cmdcc.connectTCP(self.instrument_ipaddr, int(self.instrument_ipportCmd))
        if self.CmdInstrument == None:
            log.error("Driver failed to connect to instrument command port")
            return
        else:
            log.info("Driver connected to instrument command port")
        log.info("Sending break to instrument ipaddr: %s, ipport: %s" %(self.instrument_ipaddr, self.instrument_ipportCmd))
        self.CmdData = ""
        try:
            self.CmdInstrument.transport.write('\x21\x00')   # start sending break
            yield pu.asleep(1)
            #if self.CmdData != '\x21OK':
            #    raise RuntimeError('OK response not received')
            #log.debug("Rcvd OK response for 'start break' cmd")
            self.CmdInstrument.transport.write('\x22\x00')   # stop sending break
            #if self.CmdData != '\x22OK':
            #    raise RuntimeError('OK response not received')
            #log.debug("Rcvd OK response for 'stop break' cmd")
        except:
            log.error("Send break failed: %s" % sys.exc_info()[1])
            self.Cmdproto.transport.loseConnection()
            return
        self.Cmdproto.transport.loseConnection()
        self.TimeOut = Timer(BREAK_RESPONSE_TIMEOUT, self.TimeoutCallback, "B")
        self.TimeOut.start()
        self.SendingBreak = False
                

    def ProcessRcvdData(self):
        # for now, publish a complete line at a time
        while instrument_prompts.ADCP_LINE_TERMINATOR in self.dataQueue:
            partition = self.dataQueue.partition(instrument_prompts.ADCP_LINE_TERMINATOR)
            self.dataQueue = partition[2]
            if len(self.cmdQueue) > 0:
                Cmd = self.PeekCmd()
                log.debug("looking for command %s in %s" % (Cmd, partition[0]))
                if (((Cmd == "break") and ("BREAK Wakeup A" in partition[0])) or
                    (Cmd in partition[0])):
                    if self.TimeOut != None:
                        self.TimeOut.cancel()
                        self.TimeOut = None
                    log.info("Response to command \'%s\' received" % Cmd)
                    self.dequeueCmd()
                    if self.cmdPending():
                        self.sendCmd(self.PeekCmd())
            # send this up to the agent to publish.
            log.info("Calling publish with \"%s\"" %partition[0])
            self.publish(partition[0], self.publish_to)
          
        
    def gotCmdConnected(self, instrument):
        """
        @brief This method is called when a connection has been made to the
        instrument device server.  The instrument protocol object is passed
        as a parameter, and a reference to it is saved.  
        @param reference to instrument protocol object.
        @retval none
        """
        log.debug("gotCmdConnected!!!")

        self.CmdInstrument = instrument


    def gotCmdData(self, data):
        """
        @brief The instrument command object has received data from the
        instrument command port. 
        @param data
        @retval none
        """
        
        if log.getEffectiveLevel() == logging.DEBUG:
            DataAsHex = ""
            for i in range(len(data)):
                if len(DataAsHex) > 0:
                    DataAsHex += ","
                DataAsHex += "{0:X}".format(ord(data[i]))
            log.debug("gotCmdData() [%s] [%s]" % (data, DataAsHex))
        self.CmdData = data

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
        if self.instrument == None:
            log.error("Driver failed to connect to instrument")
            self.hsm.sendEvent('eventConnectionFailed')
        else:
            log.info("Driver connected to instrument")
            self.hsm.sendEvent('eventConnectionComplete')


    def gotConnected(self, instrument):
        """
        @brief This method is called when a connection has been made to the
        instrument device server.  The instrument protocol object is passed
        as a parameter, and a reference to it is saved.  
        @param reference to instrument protocol object.
        @retval none
        """
        log.debug("gotConnected!!!")

        self.instrument = instrument


    def gotDisconnected(self, instrument):
        """
        @brief This method is called when a connection to the instrument
        device server has been lost.  The instrument protocol object is passed
        as a parameter.  
        @param reference to instrument protocol object.
        @retval none
        """
        log.debug("gotDisconnected!!!")

        self.instrument = None
        self.hsm.sendEvent('eventDisconnectComplete')


    def gotData(self, data):
        """
        @brief The instrument protocol object has received data from the
        instrument. 
        @param data
        @retval none
        """
        
        if log.getEffectiveLevel() == logging.DEBUG:
            DataAsHex = ""
            for i in range(len(data)):
                if len(DataAsHex) > 0:
                    DataAsHex += ","
                DataAsHex += "{0:X}".format(ord(data[i]))
            log.debug("gotData() [%s] [%s]" % (data, DataAsHex))
        if instrument_prompts.INST_PROMPT in data:
            log.info("gotData(): prompt seen")
            self.hsm.sendEvent('eventPromptReceived')
        self.dataQueue += data
        self.hsm.sendEvent('eventDataReceived')
        

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
        if self.TimeOut != None:
            self.TimeOut.cancel()
            self.TimeOut = None
        self.hsm.sendEvent('eventDisconnectReceived')
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
                    self.hsm.sendEvent('eventCommandReceived')
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

        log.info("op_execute content: %s" %str(content))

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
                if isinstance(instCommand, list):
                    for instCmd in instCommand:
                        log.info("op_execute queueing command: %s" % instCmd)
                        self.enqueueCmd(instCmd)
                else:
                    instCommand += value
                    log.info("op_execute queueing command: %s" % instCommand)
                    self.enqueueCmd(instCommand)
                # respond to command
                agentCommands.append(command)
                yield self.reply_ok(msg, agentCommands)
                """
                Send the command received event.  This should kick off the
                appropriate sequence of events to get the command sent.
                """
                self.hsm.sendEvent('eventCommandReceived')



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
