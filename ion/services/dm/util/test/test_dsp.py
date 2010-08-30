#!/usr/bin/env python

"""
@file ion/services/dm/distribution/test/test_baseconsumer.py
@author David Stuebe
@brief test cases for the base consumer process
"""

import logging
log = logging.getLogger(__name__)
#import time
from twisted.internet import defer
from twisted.trial import unittest
#from magnet.container import Container
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
from ion.services.dm.distribution import base_consumer
from ion.services.dm.util.data_stream_producer import DataStreamProducer


class DSPTest(IonTestCase):
    '''
    Test cases for the base consumer method. They examine the message based
    control of the child processes and ensure that ondata is called properly
    '''


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._spawn_processes(services)

        #Create two test queues
        queue1=dataobject.create_unique_identity()
        queue_properties = {queue1:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)
        self.queue1 = queue1

        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        # Kill the queues?


    @defer.inlineCallbacks
    def test_stream(self):

        #Set up a logging consumer as a listenr
        pd1={'name':'consumer_number_1',
                 'module':'ion.services.dm.distribution.consumers.logging_consumer',
                 'procclass':'LoggingConsumer',
                 'spawnargs':{'attach':[self.queue1]}}
        child1 = base_consumer.ConsumerDesc(**pd1)
        
        child1_id = yield self.test_sup.spawn_child(child1)
        
        dc1 = self._get_procinstance(child1_id)
        
        
        dsp1={'name':'data_stream_1',
                 'module':'ion.services.dm.util.data_stream_producer',
                 'procclass':'DataStreamProducer',
                 'spawnargs':{'delivery queue':self.queue1,
                              'delivery interval':5}}

        child2 = ProcessDesc(**dsp1)
        
        child2_id = yield self.test_sup.spawn_child(child2)
        
        yield pu.asleep(2)
        
        rec = dc1.receive_cnt[self.queue1]
        self.assertEqual(rec,1)
                
        yield pu.asleep(5)
        
        rec = dc1.receive_cnt[self.queue1]
        self.assertEqual(rec,2)
                
        

