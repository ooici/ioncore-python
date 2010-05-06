#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering topics for data pub sub
"""

import logging, time
from twisted.internet import defer
from twisted.trial import unittest
from magnet.container import Container
from magnet.spawnable import spawn

from ion.core.base_process import BaseProcess
from ion.services.dm.datapubsub import *
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class PubSubTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._startContainer()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stopContainer()

    @defer.inlineCallbacks
    def test_pubsub(self):
        services = [
            {'name':'datapubsub','module':'ion.services.dm.datapubsub','class':'DataPubsubservice'},
        ]

        yield self._spawnProcesses(services)
        
        sup = yield self.procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))
        
        dps = yield self.procRegistry.get("datapubsub")
        logging.info("DataPubsubservice: "+repr(dps))

        dpsc = DataPubsubClient(dps)
        topic_name = yield dpsc.define_topic("topic1")
        logging.info('Service reply: '+str(topic_name))
        
        dc1 = DataConsumer()
        dc1_id = yield spawn(dc1.receiver)
        yield dc1.attach(topic_name)
        
        dmsg = self._get_datamsg()
        yield pu.send_message(dpsc.rpc.clientRecv, '', topic_name, 'data', dmsg, {})

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)
        
        self.assertEqual(dc1.receive_cnt, 1)

        # Create a second data consumer
        dc2 = DataConsumer()
        dc2_id = yield spawn(dc2.receiver)
        yield dc2.attach(topic_name)
        
        dmsg = self._get_datamsg()
        yield pu.send_message(dpsc.rpc.clientRecv, '', topic_name, 'data', dmsg, {})

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)
        
        self.assertEqual(dc1.receive_cnt, 2)
        self.assertEqual(dc2.receive_cnt, 1)

    def _get_datamsg(self):
        return {'metadata':{},'data':'sample1\nsample2\sample3'}

class DataConsumer(BaseProcess):
        
    @defer.inlineCallbacks
    def attach(self, topic_name):
        self.dataReceiver = Receiver(__name__, topic_name)
        self.dataReceiver.handle(self.receive)
        self.dr_id = yield spawn(self.dataReceiver)
        logging.info("DataConsumer.attach "+str(self.dr_id)+" to topic "+str(topic_name))
        
        self.receive_cnt = 0
        self.received_msg = []

    def op_data(self, content, headers, msg):
        logging.info("Data message received: "+repr(content))
        self.receive_cnt += 1
        self.received_msg.append(content)


