#!/usr/bin/env python

"""
@file ion/services/dm/test/test_pubsub.py
@author Michael Meisinger
@author David Stuebe
@brief test service for registering topics for data pub sub
"""

import logging
logging = logging.getLogger(__name__)
import time
from twisted.internet import defer
from twisted.trial import unittest
from magnet.container import Container
from magnet.spawnable import Receiver
from magnet.spawnable import spawn

from ion.core import bootstrap
from ion.core.base_process import BaseProcess
from ion.services.dm.pubsub import DataPubsubClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import DAPMessageObject, \
    DataMessageObject, DictionaryMessageObject, StringMessageObject

from ion.services.dm.util import dap_tools

from pydap.model import BaseType, DapType, DatasetType, Float32, Float64, \
    GridType, Int16, Int32, SequenceData, SequenceType, StructureType, UInt16, \
    UInt32, String

import numpy

from ion.services.dm.util import dap_tools

from ion.services.dm.datapubsub import base_consumer

class MyConsumer(base_consumer.BaseConsumer):
    
    def ondata(data, notification):
        """
        Override this method
        """
        
        if hasattr(self,'queues'):
            for queue in self.queues:                
                self.send_result(queue,data,notification)


class BaseConsumerTest(IonTestCase):
    
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._spawn_processes(services)
        
        #Create two test queues
        queue1=dataobject.create_unique_identity()
        queue_properties = {queue1:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)
        self.queue1 = queue1

        queue2=dataobject.create_unique_identity()
        queue_properties = {queue2:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)
        self.queue2 = queue2


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        # Kill the queues?
        
    #@defer.inlineCallbacks
    def test_init(self):
        dc1 = MyConsumer(queues=[1,2,3],param1=3.14159)
        self.assertIn(1,dc1.queues)
        self.assertIn(2,dc1.queues)
        self.assertIn(3,dc1.queues)
        self.assertEqual(3.14159,dc1.param1)
        
    @defer.inlineCallbacks
    def test_attach(self):
        dc1 = MyConsumer()
        dc1_id = yield dc1.spawn()
        yield dc1.attach(self.queue1)
        
        self.assertEqual(dc1.receive_cnt, 0)
        self.assertEqual(dc1.consume_queue, self.queue1)
        self.assertEqual(dc1.received_msg, [])
        self.assertEqual(dc1.msgs_to_send, [])
         
        # Not working?       
        #self.failUnlessRaises(RuntimeError,dc1.attach, 'sdkfen')
        
    
    @defer.inlineCallbacks
    def test_attach_and_send(self):
        dc1 = MyConsumer()
        dc1_id = yield dc1.spawn()
        yield dc1.attach(self.queue1)
        
        dmsg = DataMessageObject()
        dmsg.notifcation = 'Junk'
        dmsg.timestamp = pu.currenttime()
        dmsg = dmsg.encode()
        
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        pu.asleep(3)
        self.assertEqual(dc1.receive_cnt, 1)
        