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

from ion.core.base_process import ProtocolFactory
from ion.core import bootstrap
from ion.core.base_process import BaseProcess, ProcessDesc
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
    
    def ondata(self, data, notification, timestamp, queues=[]):
        """
        Override this method
        """

        for queue in queues:
            self.queue_result(queue,data,notification)
                
# Spawn of the process using the module name
factory = ProtocolFactory(MyConsumer)

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
        

    @defer.inlineCallbacks
    def test_spawn_child(self):
           
        procDef={'name':'consumer_number_1', \
                 'module':'ion.services.dm.datapubsub.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child = base_consumer.ConsumerDesc(**procDef)
                
        id = yield self.test_sup.spawn_child(child)
            
        yield child.shutdown()
        
        #dc1 = MyConsumer(queues=[1,2,3],param1=3.14159)
        #self.assertIn(1,dc1.queues)
        #self.assertIn(2,dc1.queues)
        #self.assertIn(3,dc1.queues)
        #self.assertEqual(3.14159,dc1.param1)

    
    @defer.inlineCallbacks
    def test_spawn_child_attach(self):
        
        procDef={'name':'consumer_number_1', \
                 'module':'ion.services.dm.datapubsub.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child = base_consumer.ConsumerDesc(**procDef)
                
        id = yield self.test_sup.spawn_child(child)
            
        res = yield child.attach(self.queue1)
        self.assertEqual(res,'OK')
        #self.assertEqual(child.proc_attached,self.queue1)
        
        
        res = yield child.attach(None)
        self.assertEqual(res,'ERROR')
        #self.assertEqual(child.proc_attached,None)
        
        yield child.shutdown()

    @defer.inlineCallbacks
    def test_spawn_child_params(self):
        
        procDef={'name':'consumer_number_1', \
                 'module':'ion.services.dm.datapubsub.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child = base_consumer.ConsumerDesc(**procDef)
                
        id = yield self.test_sup.spawn_child(child)
            
        # Send a dictionary
        res = yield child.set_params(procDef)
        self.assertEqual(res,'OK')

        params = yield child.get_params()
        
        self.assertEqual(procDef,params)
        
        yield child.shutdown()

        

    @defer.inlineCallbacks
    def test_attach_and_data(self):
        
        pd1={'name':'consumer_number_1', \
                 'module':'ion.services.dm.datapubsub.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child1 = base_consumer.ConsumerDesc(**pd1)
                
        id = yield self.test_sup.spawn_child(child1)
        res = yield child1.attach(self.queue1)
        
        msg_cnt = yield child1.get_msg_count()
        self.assertEqual(msg_cnt,0)
        
        dmsg = DataMessageObject()
        dmsg.notifcation = 'Junk'
        dmsg.timestamp = pu.currenttime()
        dmsg = dmsg.encode()
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        self.assertEqual(msg_cnt,1)
        
        
        #Spawn another process to listen to queue 2  
        pd2={'name':'consumer_number_2', \
                 'module':'ion.services.dm.datapubsub.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child2 = base_consumer.ConsumerDesc(**pd2)
                
        id = yield self.test_sup.spawn_child(child2)
        res = yield child2.attach(self.queue2)
       
        # Tell the first consumer to pass results to the second!
        res = yield child1.set_params({'queues':[self.queue2]})
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        self.assertEqual(msg_cnt,2)
        
        msg_cnt = yield child2.get_msg_count()
        self.assertEqual(msg_cnt,1)
        
        yield child1.shutdown()
        yield child2.shutdown()
        
        
        