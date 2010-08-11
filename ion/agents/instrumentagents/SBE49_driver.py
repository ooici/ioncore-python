#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/SBE49_instrument_driver.py
@author Steve Foley
@brief Driver code for SeaBird SBE-49 CTD
"""
import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer, reactor

"""
from ion.data.dataobject import LCStates as LCS
from ion.services.base_service import BaseService
"""

from twisted.internet.protocol import Protocol, ClientFactory, ClientCreator

from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject
from ion.services.dm.distribution.pubsub_service import DataPubsubClient

from ion.agents.instrumentagents.instrument_agent import InstrumentDriver
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
        logging.debug("DHE: connectionMade, calling gotConnected().")
        self.parent.gotConnected(self)
        
    def dataReceived(self, data):
        """
        Filter the data; the instrument will send the
        prompt, which we don't care about, I'm assuming.  We might need a sort
        of state machine or something; for instance, the agent sends a getStatus
        command, we need to know that we're expecting a status message.
        """
        logging.info("DHE: dataReceived!")
        if data == 'S>':
            logging.debug("received Seabird prompt.")
            #self.factory.prompt_received(self)
            self.parent.gotPrompt(self)
        elif data == '?CMD':
            logging.info("Seabird doesn't understand command.")
        else:
            logging.debug("dataReceived()!")
            #self.factory.data_received(data)
            self.parent.gotData(data)


class SBE49InstrumentDriver(InstrumentDriver):

    connected = False
    instrument = None
    command = None
    topicDefined = False


    """    
    lifecycleState = LCS['new']
    declare = BaseService.service_declare(name = 'Instrument',
                                          version = '0.0.1',
                                          dependencies = [])
    """                                          
    
    """
    Maybe some day these values are looked up from a registry of common
        controlled vocabulary
    """
    def __init__(self, receiver=None, spawnArgs=None, **kwargs):

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

    def isConnected(self):
        return self.connected
    
    def setConnected(self, value):
        self.connected = value;

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
        
        # DHE Probably don't need to do it this way anymore 
        #self.d = defer.Deferred()
        #factory = InstrumentClientFactory(self, self.d)
        
        # Now thinking I might try clientcreator since this will only be a
        # single connection.
        #factory = InstrumentClientFactory(self)
        #self.connector = reactor.connectTCP("localhost", 9000, factory)
        logging.info("DHE: calling ClientCreator")
        cc = ClientCreator(reactor, InstrumentClient, self)
        logging.info("DHE: calling connectTCP")
        self.proto = yield cc.connectTCP("localhost", 9000)
        logging.info("DHE: connectTCP returned")

        """        
        # Instantiate a pubsubclient
        dpsc = DataPubsubClient(self)

        # Create and Register a topic
        topic = PubSubTopicResource.create('SBE49 Topic',"oceans, oil spill")        
        topic = yield dpsc.define_topic(topic)
        #topic = dpsc.define_topic(topic)
        logging.debug('DHE: Defined Topic: '+str(topic))
        """
       
        #return self.d
    
    #@defer.inlineCallbacks
    def gotConnected(self, instrument):
        """
        @brief This method is called when a connection has been made to the
        instrument device server.  The instrument protocol object is passed
        as a parameter, and a reference to it is saved.  Call setConnected
        with True argument.
        @param reference to instrument protocol object.
        @retval none
        """
        logging.debug("DHE: gotConnected!!!")

        self.instrument = instrument
        self.setConnected(True)

        """
        This is ad hoc right now.  Need a state machine or something to handle
        the possible cases (connected, not-connected)
        """
        #instrument.transport.write("ds")

        """
        # Instantiate a pubsubclient
        dpsc = DataPubsubClient(self)
        
        # Create and Register a topic
        DHE: not sure the driver should be creating the topic; for right
        now I'll have the test case do it.
        self.topic = PubSubTopicResource.create('SBE49 Topic',"oceans, oil spill")        
        self.topic = yield dpsc.define_topic(self.topic)
        #logging.debug('DHE: Defined Topic: '+str(topic))
        logging.debug('DHE: Defined Topic')
        
        self.publisher = PublisherResource.create('Test Publisher', self, self.topic, 'DataObject')
        self.publisher = yield dpsc.define_publisher(self.publisher)

        #logging.info('DHE: Defined Publisher: '+str(self.publisher))
        logging.info('DHE: Defined Publisher')

        self.topicDefined = True
        """

    def gotData(self, data):
        """
        @brief The instrument protocol object has received data from the
        instrument.  It should already be sanitized and ready for consumption;
        publish the data.
        @param data
        @retval none
        """
        # send this up to the agent to publish.
        logging.debug("gotData() %s Calling publish." % (data))
        self.publish(data, 'topic1')
      
    def gotPrompt(self, instrument):
        """
        This needs to be the general receive routine for the instrument driver
        """
        logging.debug("DHE: gotPrompt()")
        self.instrument = instrument
        self.setConnected(True)
        
        """
        Need some sort of state machine so we'll know what data we're supposed to send...
        """
        #instrument.transport.write("ds")
        if self.command != None:
            logging.debug("DHE: gotPrompt sending command: %s"  % (self.command))
            instrument.transport.write(self.command)
        else:
            logging.debug("DHE gotPrompt NOT SENDING ANYTHING")
        
    def publish(self, data, topic):
        """
        @Brief Publish the given data to the given topic.
        @param data The data to publish
        @param topic The topic to which to publish.  Currently this is not the
        topic as defined by pubsub.
        @retval none
        """
        #logging.debug("DHE: publish(): %s" %str(self.__dict__))
        logging.debug("DHE: publish()")
        if self.topicDefined == True:
            logging.debug("DHE: publishing!")

            # Create and send a data message
            result = yield dpsc.publish(self.sup, self.topic.reference(), data)
            if result:
                logging.info('DHE: Published Message')
            else:
                logging.info('DHE: Failed to Published Message')
        else:
            logging.debug("DHE: NOT READY TO PUBLISH")
        

    @defer.inlineCallbacks
    def op_initialize(self, content, headers, msg):
        logging.debug('DHE: in driver initialize')

        # Instantiate a pubsubclient
        dpsc = DataPubsubClient(self)
        
        # Create and Register a topic
        """
        DHE: not sure the driver should be creating the topic; for right
        now I'll have the test case do it.
        """
        self.topic = PubSubTopicResource.create('SBE49 Topic',"oceans, oil spill")        
        self.topic = yield dpsc.define_topic(self.topic)
        logging.debug('DHE: Defined Topic')
        
        self.publisher = PublisherResource.create('Test Publisher', self, self.topic, 'DataObject')
        self.publisher = yield dpsc.define_publisher(self.publisher)

        logging.info('DHE: Defined Publisher')

        self.topicDefined = True
        yield self.reply_ok(msg, content)
    
    @defer.inlineCallbacks
    def op_disconnect(self, content, headers, msg):
        logging.debug("DHE: in Instrument Driver op_disconnect!")
        if (self.isConnected() == True):
            logging.debug("DHE: disconnecting from instrument")
            #self.connector.disconnect()
            self.proto.transport.loseConnection()
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
        for param in content.keys():
            if (param not in self.__instrument_parameters):
                yield self.reply_err(msg, "Could not set %s" % param)
            else:
                self.__instrument_parameters[param] = content[param]
        yield self.reply_ok(msg, content)
            
    @defer.inlineCallbacks
    def op_execute(self, content, headers, msg):
        """
        Execute the given command structure (first element command, rest
        of the elements are arguments)
        @todo actually do something
        """
        assert(isinstance(content, dict))

        logging.info("DHE: in op_execute!!!")
        """
        This connection stuff could be abstracted into a communications object.
        """
        if self.isConnected() == False:
            #d = self.getConnected()
            logging.info("DHE: yielding for connect")
            yield self.getConnected()
            logging.info("DHE: connect returned")
            # DHE: using neither of these right now, but we would use only
            # one if we were using the deferred.
            #d.addCallback(self.gotConnected);
            #d.addCallback(self.gotPrompt);
            #logging.debug("waiting to be connected...")
        
        """
        DHE THERE IS A WEIRD PROBLEM WHERE getConnected() RETURNS BUT THE TOPIC ISNT DEFINED,
        BECAUSE THAT CALL YIELDS (UP IN gotConnected)
        """
        
        #while self.topicDefined != True:
        #    yield pu.asleep(1)
            
        if (content == {}):
            yield self.reply_err(msg, "Empty command")
            return
        for command in content.keys():
            if command not in instrument_commands:
                yield self.reply_err(msg, "Invalid Command")
            else:
                logging.info("DHE: command: %s" % command)
                self.command = command
                """
                This isn't working; possibly because the connection has
                not been totally set up yet.
                """
                #if self.instrument != None:
                #    logging.debug("DHE: sending command: %s" % command)
                #    self.instrument.transport.write(self.command)
        yield self.reply_ok(msg, content.keys())

    
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
        # Do something here, then adjust test case
        yield self.reply_ok(msg, content)
        
        
class SBE49InstrumentDriverClient(InstrumentDriverClient):
    """
    The client class for the instrument driver. This is the client that the
    instrument agent can use for communicating with the driver.
    """
    
    
# Spawn of the process using the module name
factory = ProtocolFactory(SBE49InstrumentDriver)
