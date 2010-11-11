#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/test/test_SBE49.py
@brief This module has test cases to test out SeaBird SBE49 instrument software
    including the driver. This assumes that generic InstrumentAgent code has
    been tested by another test case
@author Steve Foley
@see ion.agents.instrumentagents.test.test_instrument
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.test.iontest import IonTestCase

from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriverClient
from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriver
from ion.agents.instrumentagents.simulators.sim_SBE49 import Simulator
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


class TestSBE49(IonTestCase):
    
    SimulatorPort = 0


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        self.simulator = Simulator("123", 9100)
        self.SimulatorPort = self.simulator.start()
        self.assertNotEqual(self.SimulatorPort, 0)

        services = [
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'},

            {'name':'SBE49_Driver','module':'ion.agents.instrumentagents.SBE49_driver','class':'SBE49InstrumentDriver','spawnargs':{'ipport':self.SimulatorPort}}
            ]

        self.sup = yield self._spawn_processes(services)

        self.driver_pid = yield self.sup.get_child_id('SBE49_Driver')
        log.debug("Driver pid %s" % (self.driver_pid))

        self.driver_client = SBE49InstrumentDriverClient(proc=self.sup,
                                                         target=self.driver_pid)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.simulator.stop()

        # stop_SBE49_simulator(self.simproc)
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
        self.topic = PubSubTopicResource.create('SBE49 Topic',"oceans, oil spill")
        self.topic = yield dpsc.define_topic(self.topic)


        log.debug('TADA!')


    @defer.inlineCallbacks
    def test_initialize(self):
        result = yield self.driver_client.initialize('some arg')
        log.debug('TADA!')

    @defer.inlineCallbacks
    def test_driver_load(self):
        #config_vals = {'addr':'127.0.0.1', 'port':'9000'}
        config_vals = {'addr':'137.110.112.119', 'port':self.SimulatorPort}
        result = yield self.driver_client.configure_driver(config_vals)
        self.assertEqual(result['addr'], config_vals['addr'])
        self.assertEqual(result['port'], config_vals['port'])


    @defer.inlineCallbacks
    def test_fetch_set(self):
        params = {'outputformat':'2'}
        result = yield self.driver_client.set_params(params)

        params = {'baudrate':'19200'}
        result = yield self.driver_client.set_params(params)

        """
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
        subscription.topic1 = PubSubTopicResource.create('SBE49 Topic','')
        #subscription.topic2 = PubSubTopicResource.create('','oceans')

        subscription.workflow = {
            'consumer1':
                {'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'consumerclass':'LoggingConsumer',\
                 'attach':'topic1'}
                }

        subscription = yield dpsc.define_subscription(subscription)

        log.info('Defined subscription: '+str(subscription))

        #config_vals = {'ipaddr':'137.110.112.119', 'ipport':'4001'}
        config_vals = {'ipaddr':'127.0.0.1', 'ipport':self.SimulatorPort}
        result = yield self.driver_client.configure_driver(config_vals)

        cmd1 = [['ds', 'now']]
        #cmd1 = [['start', 'now']]
        #cmd2 = [['stop', 'now']]
        #cmd2 = [['pumpoff', '3600', '1']]
        yield pu.asleep(5)
        result = yield self.driver_client.execute(cmd1)
        # DHE: wait a while...
        yield pu.asleep(5)
        #result = yield self.driver_client.execute(cmd2)


        # DHE: disconnecting; a connect would probably be good.
        result = yield self.driver_client.disconnect(['some arg'])


    @defer.inlineCallbacks
    def test_sample(self):
        result = yield self.driver_client.initialize('some arg')

        dpsc = DataPubsubClient(self.sup)
        topicname = 'SBE49 Topic'
        topic = PubSubTopicResource.create(topicname,"")

        # Use the service to create a queue and register the topic
        topic = yield dpsc.define_topic(topic)

        subscription = SubscriptionResource()
        subscription.topic1 = PubSubTopicResource.create(topicname,'')

        subscription.workflow = {
            'consumer1':
                #{'module':'ion.services.dm.distribution.consumers.logging_consumer',
                {'module':'ion.agents.instrumentagents.test.inst_consumer',
                 'consumerclass':'LoggingConsumer',\
                 'attach':'topic1'}
                }

        subscription = yield dpsc.define_subscription(subscription)

        log.info('test_sample: Defined subscription: '+str(subscription))

        params = {}
        params['publish-to'] = topic.RegistryIdentity
        yield self.driver_client.configure_driver(params)

        cmd1 = [['ds', 'now']]
        result = yield self.driver_client.execute(cmd1)

        yield pu.asleep(1)

        result = yield self.driver_client.disconnect(['some arg'])

