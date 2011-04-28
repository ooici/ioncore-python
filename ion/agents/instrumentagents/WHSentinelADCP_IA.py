#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/WHSentinelADCP_driver.py
@author Bill Bollenbacher
@brief CI interface for Teledyne RDI Workhorse Sentinel ADCP
"""
'''
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import WHSentinelADCP_constants as const
from ion.agents.instrumentagents import instrument_agent as IA
from ion.agents.instrumentagents.instrument_agent import InstrumentAgent

from ion.core.process.process import ProcessFactory, ProcessDesc

from ion.resources.dm_resource_descriptions import PubSubTopicResource

# Gotta have this AFTER the "static" variables above
from ion.agents.instrumentagents.WHSentinelADCP_driver import WHSentinelADCPInstrumentDriverClient

class WHSentinelADCPInstrumentAgent(InstrumentAgent):
    """
    Workhorse Sentinel ADCP specific instrument driver
    Inherits basic get, set, getStatus, getCapabilities, etc. from parent
    """

    @defer.inlineCallbacks
    def plc_init(self):
        """
        Initialize instrument driver when this process is started.
        """
        InstrumentAgent.plc_init(self)

        self.instrument_id = self.spawn_args.get('instrument-id', '123')
        self.driver_args = self.spawn_args.get('driver-args', {'ipport':9000,'ipportCmd':9001})
        log.info("INIT agent for instrument ID: %s" % (self.instrument_id))

        self.driver_args['instrument-id'] = self.instrument_id
        self.pd = ProcessDesc(**{'name':'WHSentinelADCPDriver',
                          'module':'ion.agents.instrumentagents.WHSentinelADCP_driver',
                          'class':'WHSentinelADCPInstrumentDriver',
                          'spawnargs':self.driver_args})

        driver_id = yield self.spawn_child(self.pd)
        self.driver_client = WHSentinelADCPInstrumentDriverClient(proc=self,
                                                                  target=driver_id)

        """
        # set and register base topics - should be unique to infrastructure        
        self.output_topics = {}
        self.event_topics = {}
        self.state_topics = {}
        
        # Should these three be defined in the instrument agent?
        self.output_topics["Agent"] = PubSubTopicResource.create("OutputAgent" + self.instrument_id, "")
        self.event_topics["Agent"] = PubSubTopicResource.create("EventAgent" + self.instrument_id, "")
        self.state_topics["Agent"] = PubSubTopicResource.create("StateAgent" + self.instrument_id, "")

        self.output_topics["Device"] = PubSubTopicResource.create("WHSentinelADCPOutputDevice", "")
        self.event_topics["Device"] = PubSubTopicResource.create("WHSentinelADCPEventDevice" + self.instrument_id, "")
        self.state_topics["Device"] = PubSubTopicResource.create("WHSentinelADCPStateDevice" + self.instrument_id, "")

        for key in self.output_topics.keys():
            self.output_topics[key] = yield self.pubsub_client.define_topic(self.output_topics[key])
            log.info('Defined Topic: '+str(self.output_topics[key]))

        for key in self.event_topics.keys():
            self.event_topics[key] = yield self.pubsub_client.define_topic(self.event_topics[key])
            log.info('Defined Topic: '+str(self.event_topics[key]))

        for key in self.state_topics.keys():
            self.state_topics[key] = yield self.pubsub_client.define_topic(self.state_topics[key])
            log.info('Defined Topic: '+str(self.state_topics[key]))

        # now register for all those topics
        yield InstrumentAgent._register_publisher(self)
        """

            ### Do we even need the publisher stuff below in each define?
            #Create and register self.sup as a publisher
#            publisher = PublisherResource.create('Test Publisher', self.id,
#                                                 self.output_topics[name],
#                                                 'DataObject')
#            publisher = yield self.pubsub_client.define_publisher(publisher)
#            log.info('Defined Publisher: ' + str(publisher))
        ### Repeat above for event and state topics when the details are worked out
    
    #@defer.inlineCallbacks
    #def plc_terminate(self):
    #    yield self.pd.shutdown()

    @defer.inlineCallbacks
    def op_get_capabilities(self, content, headers, msg):
        """
        Obtain a list of capabilities that this instrument has. This is
        simply a command and parameter list at this point
        """
        yield self.reply(msg, 'get_capabilities',
                         {IA.instrument_commands: const.instrument_commands,
                          IA.instrument_parameters: const.instrument_parameters,
                          IA.ci_commands: const.ci_commands,
                          IA.ci_parameters: const.ci_parameters}, {})

    @defer.inlineCallbacks
    def op_execute_device(self, content, headers, msg):
        """
        Do any translating from instrument agent interface commands to
        individual instrument commands
        """
        log.debug("op_execute_device: command = %s", content)
        assert isinstance(content, tuple), "WHSentinelADCP executing bad command list"
        new_content = list(content)
        # Can probably be made more memory efficient with a generator
        if (content != ()) and (content[0] in const.command_substitutions):
            new_content[0] = const.command_substitutions[content[0]]
        yield InstrumentAgent.op_execute_device(self, tuple(new_content), headers, msg)

# Spawn of the process using the module name
factory = ProcessFactory(WHSentinelADCPInstrumentAgent)
'''