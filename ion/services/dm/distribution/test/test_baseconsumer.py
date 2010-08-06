#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_baseconsumer.py
@author David Stuebe
@brief test for the base consumer process
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
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import DAPMessageObject, \
    DataMessageObject, DictionaryMessageObject, StringMessageObject

from ion.services.dm.util import dap_tools


import numpy

from ion.services.dm.util import dap_tools

from ion.services.dm.distribution import base_consumer

class MyConsumer(base_consumer.BaseConsumer):

    def ondata(self, data, notification, timestamp, queues=[]):
        """
        This is an example of pulling the parameter 'queues' from spawn_args,
        'process parameters' which is passed as **kwargs to ondata
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
        
        queue3=dataobject.create_unique_identity()
        queue_properties = {queue3:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)
        self.queue3 = queue3
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        # Kill the queues?


    @defer.inlineCallbacks
    def test_spawn_attach_args(self):
        
        pd1={'name':'consumer_number_1',
                 'module':'ion.services.dm.distribution.test.test_baseconsumer',
                 'procclass':'MyConsumer',
                 'spawnargs':{'attach':[self.queue1]}}
        child1 = base_consumer.ConsumerDesc(**pd1)
        
        child1_id = yield self.test_sup.spawn_child(child1)
        
        # Don't do this - you can only get the instance in a test case -
        # this is not a valid pattern in OTP
        dc1 = self._get_procinstance(child1_id)
        
        self.assertIn(self.queue1,dc1.dataReceivers)


    @defer.inlineCallbacks
    def test_spawn_attach_msg(self):
        
        pd1={'name':'consumer_number_1', \
                 'module':'ion.services.dm.distribution.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child1 = base_consumer.ConsumerDesc(**pd1)
                
        child1_id = yield self.test_sup.spawn_child(child1)
            
        res = yield child1.attach(self.queue1)
        self.assertEqual(res,'OK')
        #self.assertEqual(child.proc_attached,self.queue1)
        
        
        res = yield child1.attach(None)
        self.assertEqual(res,'ERROR')
        #self.assertEqual(child.proc_attached,None)
        
        yield child1.shutdown()

    @defer.inlineCallbacks
    def test_spawn_attach_inst(self):
        
        pd1={'name':'consumer_number_1', \
                 'module':'ion.services.dm.distribution.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child1 = base_consumer.ConsumerDesc(**pd1)
                
        child1_id = yield self.test_sup.spawn_child(child1)
            
        # NOT VALID IN OTP TO GET THE INSTANCE ONLY FOR TEST CASE
        dc1 = self._get_procinstance(child1_id)
            
        res = yield dc1.attach(self.queue1)
        #@Todo Assert what?
        self.assert_(res)
        #self.assertEqual(child.proc_attached,self.queue1)
        
        yield dc1.shutdown()



    @defer.inlineCallbacks
    def test_params(self):
        
        pd1={'name':'consumer_number_1', \
                 'module':'ion.services.dm.distribution.test.test_baseconsumer', \
                 'procclass':'MyConsumer'}
        child1 = base_consumer.ConsumerDesc(**pd1)
                
        child1_id = yield self.test_sup.spawn_child(child1)
            
        # Send a dictionary
        params={'Junk':'Trunk'}
        res = yield child1.set_process_parameters(params)
        self.assertEqual(res,'OK')

        res = yield child1.get_process_parameters()
        
        self.assertEqual(res,params)
        
        yield child1.shutdown()


    @defer.inlineCallbacks
    def test_send(self):
        pd1={'name':'consumer_number_1',
                 'module':'ion.services.dm.distribution.test.test_baseconsumer',
                 'procclass':'MyConsumer',
                 'spawnargs':{'attach':self.queue1,\
                              'Process Parameters':{'queues':[self.queue2]}}\
                    }
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

        dmsg = DataMessageObject()
        dmsg.notifcation = 'Junk'
        dmsg.timestamp = pu.currenttime()
        dmsg = dmsg.encode()
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(self.queue2),1)
        self.assertEqual(received.get(self.queue1),1)


    @defer.inlineCallbacks
    def test_send_chain(self):
        pd1={'name':'consumer_number_1',
                 'module':'ion.services.dm.distribution.test.test_baseconsumer',
                 'procclass':'MyConsumer',
                 'spawnargs':{'attach':self.queue1,\
                              'Process Parameters':{'queues':[self.queue2]}}\
                    }
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

        dmsg = DataMessageObject()
        dmsg.notifcation = 'Junk'
        dmsg.timestamp = pu.currenttime()
        dmsg = dmsg.encode()
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(self.queue2),1)
        self.assertEqual(received.get(self.queue1),1)
        
        
        #Spawn another process to listen to queue 2  
        pd2={'name':'consumer_number_2', \
                 'module':'ion.services.dm.distribution.test.test_baseconsumer', \
                 'procclass':'MyConsumer',\
                 'spawnargs':{'attach':self.queue2}}
        
        child2 = base_consumer.ConsumerDesc(**pd2)
                
        child2_id = yield self.test_sup.spawn_child(child2)
       
        # Tell the first consumer to pass results to the second!
        #res = yield child1.set_params({'queues':[self.queue2]})
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(self.queue2),2)
        self.assertEqual(received.get(self.queue1),2)
        
        
        msg_cnt = yield child2.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent')
        self.assertEqual(sent,{})
        self.assertEqual(received.get(self.queue2),1)
        
        
        yield child1.shutdown()
        yield child2.shutdown()
        
        
        
        
    @defer.inlineCallbacks
    def test_attach2_send(self):
        
        pd1={'name':'consumer_number_1',
                 'module':'ion.services.dm.distribution.test.test_baseconsumer',
                 'procclass':'MyConsumer',
                 'spawnargs':{'attach':[self.queue1, self.queue2],\
                    'Process Parameters':{'queues':[self.queue3]}}\
            }
            
        child1 = base_consumer.ConsumerDesc(**pd1)
        
        child1_id = yield self.test_sup.spawn_child(child1)
        
        dc1 = self._get_procinstance(child1_id)
        
        self.assertIn(self.queue1,dc1.dataReceivers)
        self.assertIn(self.queue2,dc1.dataReceivers)
        
        dmsg = DataMessageObject()
        dmsg.notifcation = 'Junk'
        dmsg.timestamp = pu.currenttime()
        dmsg = dmsg.encode()
        
        # Send to queue1
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(self.queue3),1)
        self.assertEqual(received.get(self.queue1),1)
        
        # Send to queue2
        yield self.test_sup.send(self.queue2, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(self.queue3),2)
        self.assertEqual(received.get(self.queue1),1)
        self.assertEqual(received.get(self.queue2),1)
        
        yield child1.shutdown()
        
        
        
    @defer.inlineCallbacks
    def test_deattach(self):
        pd1={'name':'consumer_number_1',
                 'module':'ion.services.dm.distribution.test.test_baseconsumer',
                 'procclass':'MyConsumer',
                 'spawnargs':{'attach':self.queue1,\
                              'Process Parameters':{'queues':[self.queue2]}}\
                    }
        child1 = base_consumer.ConsumerDesc(**pd1)

        child1_id = yield self.test_sup.spawn_child(child1)

        dmsg = DataMessageObject()
        dmsg.notifcation = 'Junk'
        dmsg.timestamp = pu.currenttime()
        dmsg = dmsg.encode()
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(self.queue2),1)
        self.assertEqual(received.get(self.queue1),1)
        
        raise unittest.SkipTest("Magnet does not yet support deattach.")
        
        res = yield child1.deattach(self.queue1)
        self.assertEqual(res,'OK')
        
        yield self.test_sup.send(self.queue1, 'data', dmsg)
        
        msg_cnt = yield child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent.get(self.queue2),1)
        self.assertEqual(received.get(self.queue1),1)
        
        
        
        

        