#!/usr/bin/env python
"""
@file ion/agents/test/test_SBE49.py
@brief This module has test cases to test out SeaBird SBE49 instrument software
    including the driver. This assumes that generic InstrumentAgent code has
    been tested by another test case
@author Steve Foley
@see ion.agents.instrumentagents.test.test_instrument
"""
import logging
from twisted.internet import defer
from ion.test.iontest import IonTestCase

from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriverClient
from ion.agents.instrumentagents.SBE49_driver import SBE49InstrumentDriver
from ion.core import bootstrap

from magnet.spawnable import Receiver
from magnet.spawnable import spawn
from ion.core.base_process import BaseProcess, ProcessDesc
from ion.services.dm.distribution.pubsub_service import DataPubsubClient
from ion.services.base_service import BaseServiceClient

from ion.services.dm.distribution import base_consumer
from ion.services.dm.distribution.consumers import forwarding_consumer
from ion.services.dm.distribution.consumers import logging_consumer
from ion.services.dm.distribution.consumers import example_consumer

import ion.util.procutils as pu
from ion.data import dataobject
#from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource
from subprocess import Popen, PIPE
import os

from twisted.trial import unittest

def start_SBE49_simulator():
    """
    Construct the path to the instrument simulator, starting with the current
    working directory
    """
    cwd = os.getcwd()
    myPid = os.getpid()
    logging.debug("DHE: myPid: %s" % (myPid))

    simDir = cwd.replace("_trial_temp", "ion/agents/instrumentagents/test/")
    #simPath = simDir("sim_SBE49.py")
    simPath = simDir + "sim_SBE49.py"
    #logPath = simDir.append("sim.log")
    logPath = simDir + "sim.log"
    logging.info("cwd: %s, simPath: %s, logPath: %s" %(str(cwd), str(simPath), str(logPath)))
    simLogObj = open(logPath, 'a')
    #self.simProc = Popen(simPath, stdout=PIPE)
    simProc = Popen(simPath, stdout=simLogObj)
    return simProc

def stop_SBE49_simulator(simproc):
    simproc.terminate()


class TestSBE49(IonTestCase):


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        # Start the simulator
        logging.info("Starting instrument simulator.")

        self.simproc = start_SBE49_simulator()

        # Sleep for a while to allow simlator to get set up.
        yield pu.asleep(1)

        services = [
            {'name':'pubsub_registry','module':'ion.services.dm.distribution.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'pubsub_service','module':'ion.services.dm.distribution.pubsub_service','class':'DataPubsubService'}
            ]

        #self.pubsubSuper = yield self._spawn_processes(services)
        self.sup = yield self._spawn_processes(services)

        #self.sup = yield bootstrap.create_supervisor()

        child_id = yield self.sup.get_child_id('pubsub_service')
        logging.debug("DHE: PubSub Test Service ID: " + str(child_id))
        self.pubsub = self._get_procinstance(child_id)
        logging.debug("DHE: got procinstance")

        driverParms = {'name':'SBE49_Driver',
                 'module':'ion.agents.instrumentagents.SBE49_driver',
                 'procclass':'SBE49InstrumentDriver'
                }
        driverEgg = ProcessDesc(**driverParms)

        self.driver_pid = yield self.sup.spawn_child(driverEgg)
        print("pid %s" % (self.driver_pid))

        #self.driver = SBE49InstrumentDriver()
        #self.driver_pid = yield self.driver.spawn()

        #yield self.driver.init()

        self.driver_client = SBE49InstrumentDriverClient(proc=self.sup,
                                                         target=self.driver_pid)
        #self.driver_client = SBE49InstrumentDriverClient(proc=self.sup,
        #                                                 target="SBE49_Driver")

    @defer.inlineCallbacks
    def tearDown(self):
        logging.info("Stopping instrument simulator.")
        stop_SBE49_simulator(self.simproc)
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


        print 'TADA!'


    @defer.inlineCallbacks
    def test_initialize(self):
        #dpsc = DataPubsubClient(self.pubsubSuper)

        result = yield self.driver_client.initialize('some arg')
        yield pu.asleep(4)
        print 'TADA!'



    @defer.inlineCallbacks
    def test_driver_load(self):
        config_vals = {'addr':'127.0.0.1', 'port':'9000'}
        result = yield self.driver_client.configure_driver(config_vals)
        self.assertEqual(result['status'], 'OK')
        self.assertEqual(result['addr'], config_vals['addr'])
        self.assertEqual(result['port'], config_vals['port'])


    @defer.inlineCallbacks
    def test_fetch_set(self):
        """
        params = {'baudrate':'19200', 'outputsal':'N'}
        result = yield self.driver_client.fetch_params(params.keys())
        self.assertNotEqual(params, result)
        result = yield self.driver_client.set_params({})
        self.assertEqual(len(result.keys()), 1)
        self.assertEqual(result['status'], 'OK')
        set_result = yield self.driver_client.set_params(params)
        self.assertEqual(set_result['status'], 'OK')
        self.assertEqual(set_result['baudrate'], params['baudrate'])
        self.assertEqual(set_result['outputsal'], params['outputsal'])
        result = yield self.driver_client.fetch_params(params.keys())
        self.assertEqual(result['status'], 'OK')
        self.assertEqual(result['baudrate'], params['baudrate'])
        self.assertEqual(result['outputsal'], params['outputsal'])
        """

        raise unittest.SkipTest('Temporarily skipping')


    @defer.inlineCallbacks
    def test_execute(self):
        """
        Lame test since this doesnt do much
        """
        result = yield self.driver_client.initialize('some arg')
        yield pu.asleep(4)

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

        subscription = yield self.pubsub.create_subscription(subscription)

        logging.info('Defined subscription: '+str(subscription))

        """

        # Create and Register a topic
        topic = PubSubTopicResource.create('Daves Topic',"surfing, sailing, diving")
        topic = yield dpsc.define_topic(topic)
        logging.info('Defined Topic: '+str(topic))

        #Create and register self.sup as a publisher
        print 'SUP',self.pubsubSuper,self.test_sup

        publisher = PublisherResource.create('Test Publisher', self.sup, topic, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        logging.info('Defined Publisher: '+str(publisher))
        """

        # === Create a Consumer and queues - this will become part of define_subscription.

        #Create two test queues - don't use topics to test the consumer
        # To be replaced when the subscription service is ready
        """
        queue1 = dataobject.create_unique_identity()
        queue_properties = {queue1:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

        queue2 = dataobject.create_unique_identity()
        queue_properties = {queue2:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)

        pd1={'name':'example_consumer_1',
                 'module':'ion.services.dm.distribution.consumers.forwarding_consumer',
                 'procclass':'ForwardingConsumer',
                 'spawnargs':{'attach':topic.queue.name,\
                              'Process Parameters':\
                              {'queues':[queue1,queue2]}}\
                    }
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

        # === End to be replaces with Define_Consumer
        """

        cmd1 = [['ds', 'now']]
        #cmd1 = [['start', 'now']]
        #cmd2 = [['stop', 'now']]
        #cmd2 = [['pumpoff', '3600', '1']]
        result = yield self.driver_client.execute(cmd1)
        self.assertEqual(result['status'], 'OK')
        # DHE: wait a while...
        yield pu.asleep(5)
        #result = yield self.driver_client.execute(cmd2)
        #self.assertEqual(result['status'], 'OK')


        # DHE: disconnecting; a connect would probably be good.
        result = yield self.driver_client.disconnect(['some arg'])


class DataConsumer(BaseProcess):
    """
    A class for spawning as a separate process to consume the responses from
    the instrument.
    """

    @defer.inlineCallbacks
    def attach(self, topic_name):
        """
        Attach to the given topic name
        """
        yield self.init()
        self.dataReceiver = Receiver(__name__, topic_name)
        self.dataReceiver.handle(self.receive)
        self.dr_id = yield spawn(self.dataReceiver)

        self.receive_cnt = 0
        self.received_msg = []
        self.ondata = None

    @defer.inlineCallbacks
    def op_data(self, content, headers, msg):
        """
        Data has been received.  Increment the receive_cnt
        """
        self.receive_cnt += 1
        self.received_msg.append(content)
