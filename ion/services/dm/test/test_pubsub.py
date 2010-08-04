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

from ion.core.base_process import BaseProcess
from ion.services.dm.pubsub import DataPubsubClient
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import Publication, PublisherResource, PubSubTopicResource, SubscriptionResource, DAPMessageObject

from ion.services.dm.util import dap_tools

from pydap.model import BaseType, DapType, DatasetType, Float32, Float64, \
    GridType, Int16, Int32, SequenceData, SequenceType, StructureType, UInt16, \
    UInt32, String

import numpy

from ion.services.dm.util import dap_tools


class PubSubTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'datapubsub_registry','module':'ion.services.dm.datapubsub.pubsub_registry','class':'DataPubSubRegistryService'},
            {'name':'data_pubsub','module':'ion.services.dm.pubsub','class':'DataPubsubService'}
            ]

        self.sup = yield self._spawn_processes(services)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_pubsub(self):

        dpsc = DataPubsubClient(self.sup)
        
        # Create and Register a topic
        topic = PubSubTopicResource.create('Davids Topic',"oceans, oil spill, fun things to do")        
        topic = yield dpsc.define_topic(topic)
        logging.info('Defined Topic: '+str(topic))

    
        #Create and register self.sup as a publisher
        publisher = PublisherResource.create('Test Publisher', self.sup, topic, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        logging.info('Defined Publisher: '+str(publisher))

        dc1 = DataConsumer()
        dc1_id = yield dc1.spawn()
        # @Todo replace attach with subscribe!
        yield dc1.attach(topic)


        # Create and send a data message
        data = {'Data':'in a dictionary'}
        result = yield dpsc.publish(self.sup, topic.reference(), data)
        if result:
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        self.assertEqual(dc1.receive_cnt, 1)

        # Create a second data consumer
        dc2 = DataConsumer()
        dc2_id = yield dc2.spawn()
        yield dc2.attach(topic)

        # Create a DAP data set and send that!
        dmsg = dap_tools.simple_datamessage(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)})
        result = yield dpsc.publish(self.sup, topic.reference(), dmsg)
        
        
        
        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        self.assertEqual(dc1.receive_cnt, 2)
        self.assertEqual(dc2.receive_cnt, 1)


        # Create and send a data message - a simple string
        data = 'a string of data' # usually not a great idea!
        result = yield dpsc.publish(self.sup, topic.reference(), data)
        if result:
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')

        # Need to await the delivery of data messages into the (separate) consumers
        yield pu.asleep(1)

        self.assertEqual(dc1.receive_cnt, 3)
        self.assertEqual(dc2.receive_cnt, 2)




    @defer.inlineCallbacks
    def test_chainprocess(self):
        # This test covers a chain of three data consumer processes on three
        # topics. One process is an event-detector and data-filter, sending
        # event messages to an event queue and a new data message to a different
        # data queue


        dpsc = DataPubsubClient(self.sup)
        
        #Create and register 3 topics!
        topic_raw = PubSubTopicResource.create("topic_raw","oceans, oil spill, fun things to do") 
        topic_raw = yield dpsc.define_topic(topic_raw)

        topic_qc = PubSubTopicResource.create("topic_qc","oceans_qc, oil spill") 
        topic_qc = yield dpsc.define_topic(topic_qc)
        
        topic_evt = PubSubTopicResource.create("topic_evt", "spill events")
        topic_evt = yield dpsc.define_topic(topic_evt)


        #Create and register self.sup as a publisher
        publisher = PublisherResource.create('Test Publisher', self.sup, topic_raw, 'DataObject')
        publisher = yield dpsc.define_publisher(publisher)

        logging.info('Defined Publisher: '+str(publisher))

        dc1 = DataConsumer()
        dc1_id = yield dc1.spawn()
        # Use subscribe - does not exist yet
        yield dc1.attach(topic_raw)
        
        dc1.set_ondata(e_process,topic_qc,topic_evt)
        

        dc2 = DataConsumer()
        dc2_id = yield dc2.spawn()
        # Use subscribe - does not exist yet
        yield dc2.attach(topic_qc)

        dc3 = DataConsumer()
        dc3_id = yield dc3.spawn()
        # Use subscribe - does not exist yet
        yield dc3.attach(topic_evt)

        # Create an example data message with time
        dmsg = dap_tools.simple_datamessage(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(101,102,103,104,105,106,107,108,109,110), \
            'height':(5,2,4,5,-1,9,3,888,3,4)})
        
        result = yield dpsc.publish(self.sup, topic_raw.reference(), dmsg)
        if result:
            logging.info('Published Message')
        else:
            logging.info('Failed to Published Message')


        # Need to await the delivery of data messages into the consumers
        yield pu.asleep(2)

        # asser that the correct number of message have been received
        self.assertEqual(dc1.receive_cnt, 1)
        self.assertEqual(dc2.receive_cnt, 1)
        self.assertEqual(dc3.receive_cnt, 2)

        # Publish a second message with different data
        dmsg = dap_tools.simple_datamessage(\
            {'DataSet Name':'Simple Data','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)})
        
        result = yield dpsc.publish(self.sup, topic_raw.reference(), dmsg)

        # Need to await the delivery of data messages into the consumers
        yield pu.asleep(2)


        # Assert that the correct number of message have been received
        self.assertEqual(dc1.receive_cnt, 2)
        self.assertEqual(dc2.receive_cnt, 2)
        self.assertEqual(dc3.receive_cnt, 4)



def e_process(content,headers,topic1,topic2):
    # This is a data processing function that takes a sample message, filters for   
    # out of range values, adds to an event queue, and computes a new sample packet
    # where outlyers are zero'ed out
    #print "In data process"
    

    logging.debug('e_process recieved:' + str(content))
    # Convert the message to a DAPMessageObject
    dap_msg = dataobject.DataObject.decode(content)
    logging.debug('DAP Data:'+str(dap_msg))
    # Read the message object and convert to a pydap object
    data = dap_tools.dap_msg2ds(dap_msg)
    
    resdata = []
    messages = []
    # Process the array of data
    for ind in range(data.height.shape[0]):
        
        ts = data.time[ind]
        samp = data.height[ind]
        if samp<0 or samp>100:
            # Must convert pydap/numpy Int32 to int!
            messages.append((topic2,{'event':(int(ts),'out_of_range',int(samp))}))
            samp = 0
        newsamp = samp * samp
        # Must convert pydap/numpy Int32 to int!
        ds = (int(ts),int(newsamp))
        resdata.append(ds)
    
    # Messages contains a list of tuples, (topic, data) to send 
    messages.append((topic1,{'metadata':{},'data':resdata}))
    
    logging.debug("messages" + str(messages))
    return messages



class DataConsumer(BaseProcess):

    def set_ondata(self, ondata,topic1,topic2):
        self.ondata = ondata
        self.topic1 = topic1
        self.topic2 = topic2

    @defer.inlineCallbacks
    def attach(self, topic):
        yield self.init()
        self.dataReceiver = Receiver(__name__, topic.queue.name)
        self.dataReceiver.handle(self.receive)
        self.dr_id = yield spawn(self.dataReceiver)
        logging.info("DataConsumer.attach "+str(self.dr_id)+" to topic "+str(topic.queue.name))

        self.receive_cnt = 0
        self.received_msg = []
        self.ondata = None

    @defer.inlineCallbacks
    def op_data(self, content, headers, msg):
        logging.info("Data message received: "+repr(content))
        self.receive_cnt += 1
        self.received_msg.append(content)
        if hasattr(self, 'ondata') and self.ondata:
            logging.info("op_data: Executing data process")
            res = self.ondata(content, headers,self.topic1,self.topic2)
            logging.info("op_data: Finished data process")
            #print 'RES',res
            if res:
                for (topic, msg) in res:
                    #print 'TKTKTKTKTKTKTKT'
                    #print topic
                    #print msg
                    yield self.send(topic.queue.name, 'data', msg, {})

'''
class DataProcess(object):

    def __init__(self, procdef):
        self.proc_def = procdef

    def get_ondata(self):
        return self.execute_process

    def execute_process(self, content, headers):
        loc = {'content':content,'headers':headers}
        exec self.proc_def in globals(), loc
        if 'result' in loc:
            return loc['result']
        return None

proc = """
# This is a data processing function that takes a sample message, filters for
# out of range values, adds to an event queue, and computes a new sample packet
# where outlyers are zero'ed out
#print "In data process"
data = content['data']
resdata = []
messages = []
for (ts,samp) in data:
    if samp<0 or samp>100:
        messages.append(('topic_qcevent',{'event':(ts,'out_of_range',samp)}))
        samp = 0
    newsamp = samp * samp
    ds = (ts,newsamp)
    resdata.append(ds)

messages.append(('topic_qc',{'metadata':{},'data':resdata}))

#print "messages", messages
result = messages
"""
'''

