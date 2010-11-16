#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/test/test_WHSentinelADCP.py
@brief This module has test cases to test out the Workhorse Sentinel ADCP
    instrument software of the driver. This assumes that generic InstrumentAgent
    code has been tested by another test case
@author Bill Bollenbacher
@see ion.agents.instrumentagents.test.test_instrument
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.test.iontest import IonTestCase

from ion.agents.instrumentagents.WHSentinelADCP_driver import WHSentinelADCPInstrumentDriverClient
from ion.agents.instrumentagents.WHSentinelADCP_driver import WHSentinelADCPInstrumentDriver
from ion.agents.instrumentagents.simulators.sim_WHSentinelADCP import Simulator
from ion.core import bootstrap

from ion.core.messaging.receiver import Receiver
from ion.core.process.process import Process, ProcessDesc
from ion.services.dm.distribution.pubsub_service import DataPubsubClient

from ion.services.dm.distribution import base_consumer
from ion.services.dm.distribution.consumers import forwarding_consumer
from ion.services.dm.distribution.consumers import logging_consumer
from ion.services.dm.distribution.consumers import example_consumer

import ion.util.procutils as pu
from ion.data import dataobject
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource

from twisted.trial import unittest


class TestWHSentinelADCP(IonTestCase):
    
    SimulatorPort = 0

    @defer.inlineCallbacks
    def setUp(self):
        log.debug("In TestWHSentinelADCP.setUp()")
        yield self._start_container()

        self.simulator = Simulator("123", 9100)
        self.SimulatorPort = self.simulator.start()
        self.assertNotEqual(self.SimulatorPort, 0)

        services = [
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'},

            {'name':'WHSentinelADCP_Driver','module':'ion.agents.instrumentagents.WHSentinelADCP_driver','class':'WHSentinelADCPInstrumentDriver','spawnargs':{'ipport':self.SimulatorPort,'ipportCmd':967}}
            ]

        self.sup = yield self._spawn_processes(services)

        self.driver_pid = yield self.sup.get_child_id('WHSentinelADCP_Driver')
        log.debug("Driver pid %s" % (self.driver_pid))

        self.driver_client = WHSentinelADCPInstrumentDriverClient(proc=self.sup,
                                                         target=self.driver_pid)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.simulator.stop()

        # stop_WHSentinelADCP_simulator(self.simproc)
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_create_topic(self):
        #dpsc = DataPubsubClient(self.pubsubSuper)

        dpsc = DataPubsubClient(self.sup)
        # Create and Register a topic
        """
        DHE: not sure the driver should be creating the topic; for right
        now I'll have the test case do it.
        """
        self.topic = PubSubTopicResource.create('WHSentinelADCP Topic',"oceans, oil spill")
        self.topic = yield dpsc.define_topic(self.topic)

        log.debug('TADA!')


    @defer.inlineCallbacks
    def test_initialize(self):
        result = yield self.driver_client.initialize('some arg')
        log.debug('TADA!')

    @defer.inlineCallbacks
    def test_driver_load(self):
        #config_vals = {'ipaddr':'127.0.0.1', 'ipport':'9000'}
        #config_vals = {'ipaddr':'137.110.112.119', 'ipport':self.SimulatorPort}
        result = yield self.driver_client.configure_driver(config_vals)
        self.assertEqual(result['ipaddr'], config_vals['ipaddr'])
        self.assertEqual(result['ipport'], config_vals['ipport'])


    @defer.inlineCallbacks
    def test_fetch_set(self):
        params = {'baudrate':'19200'}
        result = yield self.driver_client.set_params(params)

        """
        params = {'outputformat':'2'}
        result = yield self.driver_client.set_params(params)
        params = {'baudrate':'19200', 'outputsal':'N'}
        result = yield self.driver_client.fetch_params(params.keys())
        self.assertNotEqual(params, result)
        result = yield self.driver_client.set_params({})
        self.assertEqual(len(result.keys()), 1)
        set_result = yield self.driver_client.set_params(params)
        self.assertEqual(set_result['baudrate'], params['baudrate'])
        self.assertEqual(set_result['outputsal'], params['outputsal'])
        result = yield self.driver_client.fetch_params(params.keys())
        self.assertEqual(result['baudrate'], params['baudrate'])
        self.assertEqual(result['outputsal'], params['outputsal'])
        """

        #raise unittest.SkipTest('Temporarily skipping')


    @defer.inlineCallbacks
    def test_execute(self):
        """
        Test the execute command to the Instrument Driver
        """
        result = yield self.driver_client.initialize('some arg')

        dpsc = DataPubsubClient(self.sup)

        subscription = SubscriptionResource()
        subscription.topic1 = PubSubTopicResource.create('WHSentinelADCP Topic','')
        #subscription.topic2 = PubSubTopicResource.create('','oceans')

        subscription.workflow = {
            'consumer1':
                {'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'consumerclass':'LoggingConsumer',\
                 'attach':'topic1'}
                }

        subscription = yield dpsc.define_subscription(subscription)

        log.info('Defined subscription: '+str(subscription))

        #config_vals = {'ipaddr':'137.110.112.119', 'ipport':'4002', 'ipportCmd':967}
        #config_vals = {'ipaddr':'127.0.0.1', 'ipport':self.SimulatorPort, 'ipportCmd':967}
        result = yield self.driver_client.configure_driver(config_vals)


        cmd1 = [['cr', '1']]
        yield pu.asleep(5)

        result = yield self.driver_client.execute(cmd1)
        # wait a while...
        yield pu.asleep(5)
        #result = yield self.driver_client.execute(cmd2)


        # DHE: disconnecting; a connect would probably be good.
        result = yield self.driver_client.disconnect(['some arg'])


    """
    @defer.inlineCallbacks
    def test_sample(self):
        result = yield self.driver_client.initialize('some arg')

        dpsc = DataPubsubClient(self.sup)
        topicname = 'WHSentinelADCP Topic'
        topic = PubSubTopicResource.create(topicname,"")

        # Use the service to create a queue and register the topic
        topic = yield dpsc.define_topic(topic)

        subscription = SubscriptionResource()
        subscription.topic1 = PubSubTopicResource.create(topicname,'')

        subscription.workflow = {
            'consumer1':
                {'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'consumerclass':'LoggingConsumer',\
                 'attach':'topic1'}
                }

        subscription = yield dpsc.define_subscription(subscription)

        log.info('Defined subscription: '+str(subscription))

        params = {}
        params['publish-to'] = topic.RegistryIdentity
        yield self.driver_client.configure_driver(params)

        cmd1 = [['ds', 'now']]
        result = yield self.driver_client.execute(cmd1)

        yield pu.asleep(1)

        result = yield self.driver_client.disconnect(['some arg'])
    """

class DataConsumer(Process):
    """
    A class for spawning as a separate process to consume the responses from
    the instrument.
    """

    @defer.inlineCallbacks
    def attach(self, topic_name):
        """
        Attach to the given topic name
        """
        self.dataReceiver = Receiver(name=topic_name, handler=self.receive)
        yield self.dataReceiver.attach()

        self.receive_cnt = 0
        self.received_msg = []
        self.ondata = None

    @defer.inlineCallbacks
    def op_data(self, content, headers, msg):
        """
        Data has been received.  Increment the receive_cnt
        """
        log.debug("@@@@@@@@@@@@@@@@@@@@@@@@ data received: %s" %s(str(self.received_msg)))
        self.receive_cnt += 1
        self.received_msg.append(content)
