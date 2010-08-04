#!/usr/bin/env python


"""
@file ion/services/dm/datapubsub/base_consumer.py
@author David Stuebe
@brief the base class for pubsub consumer processes
"""

import logging
logging = logging.getLogger(__name__)

import time

from twisted.internet import defer

from magnet.container import Container
from magnet.spawnable import Receiver
from magnet.spawnable import spawn

from ion.core.base_process import BaseProcess
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import PubSubTopicResource, \
    DAPMessageObject, DataMessageObject, StringMessageObject, DictionaryMessageObject

from ion.services.dm.util import dap_tools

from pydap.model import BaseType, DapType, DatasetType, Float32, Float64, \
    GridType, Int16, Int32, SequenceData, SequenceType, StructureType, UInt16, \
    UInt32, String

import numpy

from ion.services.dm.util import dap_tools


class BaseConsumer(BaseProcess):

    '''
    def __init__(self,**kwargs):
        """
        @Brief Initialize the consumer with parameters:
        Example:
        queue=queue_name, param1=3.14159
        @Note - Becareful not to bork attrs of Base Process with your keyword args!
        """
        
        BaseProcess.__init__(self)

        for k,v in kwargs.items():
            setattr(self,k,v)
    '''
    

    @defer.inlineCallbacks
    def attach(self, consume_queue):
        yield self.init()
        self.consume_queue = consume_queue
        self.dataReceiver = Receiver(__name__, consume_queue)
        self.dataReceiver.handle(self.receive)
        self.dr_id = yield spawn(self.dataReceiver)
        logging.info("DataConsumer.attach "+str(self.dr_id)+" to topic "+str(consume_queue))

        self.receive_cnt = 0
        self.received_msg = []
        self.msgs_to_send=[]

    @defer.inlineCallbacks
    def op_data(self, content, headers, msg):

        logging.debug(self.__class__.__name__ +', MSG Received: ' + str(headers))

        logging.info(self.__class__.__name__ + '; Calling data process!')

        # Keep a record of messages received
        #@Note this could get big! What todo?
        self.receive_cnt += 1
        self.received_msg.append(content)
        
        # Unpack the message and turn it into data
        datamessage = dataobject.DataObject.decode(content)
        
        if isinstance(datamessage, DAPMessageObject):
            data = dap_tools.dap_msg2ds(datamessage)
        elif isinstance(datamessage, (StringMessageObject, DictionaryMessageObject)):
            data = datamessage.data
        else:
            data = None
            
        notification = datamessage.notification
        timestamp = datamessage.timestamp
        
        res = self.ondata(data, notification,timestamp)
        logging.info(self.__class__.__name__ +"; op_data: Finished data processing")

        if self.msgs_to_send:
            for ind in range(len(self.msgs_to_send)):
                queue, msg = self.msgs_to_send.pop[0]
                yield self.send(queue, 'data', msg) 
    
            logging.info(self.__class__.__name__ +"; op_data: Finished sending results")


    def ondata(data, notification,timestamp):
        """
        Override this method
        """
        raise NotImplementedError, "BaseConsumer class does not implement ondata"
        
        
    def send_result(queue, data=None, notification=''):
        
        if isinstance(data, DatasetType):
            msg = dap_tools.ds2dap_msg(data)
        elif isinstance(data, str):
            msg = StringMessageObject()
            msg.data=data
        elif isinstance(data, dict):
            msg = DictionaryMessageObject()
            msg.data=data
        elif not data:
            msg = DataMessageObject()
        else:
            raise RuntimeError('Invalid data type passed to send_result in class %s: type:' % (self.__class__.__name__, type(data)))
        
        if not notification:
            notification = self.__class__.__name__ + ' received a message!'
            
        msg.notification = notification
        
        msg.timestamp = pu.currenttime()
        
        self.msgs_to_send.append((queue, msg))
        
        
        