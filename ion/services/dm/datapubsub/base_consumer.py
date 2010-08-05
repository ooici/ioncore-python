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

from ion.core.base_process import BaseProcess, ProcessDesc
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
    
    @defer.inlineCallbacks
    def op_attach(self, content, headers, msg):
        '''
        Message interface to attach to another queue
        '''
        logging.info(self.__class__.__name__ +'; Calling Attach; Queue:' + str(content))
        queue = content.get('queue',None)
        if not queue:
            yield self.reply_err(msg)
            return
        
        self.attached_queue = queue
        self.dataReceiver = Receiver(__name__, queue)
        self.dataReceiver.handle(self.receive)
        self.dr_id = yield spawn(self.dataReceiver)
        logging.info("DataConsumer.attach "+str(self.dr_id)+" to topic "+str(queue))

        self.receive_cnt = 0
        self.received_msg = []
        self.msgs_to_send=[]
        
        yield self.reply_ok(msg)
        
    @defer.inlineCallbacks
    def op_set_params(self, content, headers, msg):
        '''
        Message interface to set parameters
        '''
        logging.info(self.__class__.__name__ +'; Calling Set Params; Params:' + str(content))
        if not content:
            yield self.reply_err(msg)
            return
        self.params = content
        yield self.reply_ok(msg)
        
    @defer.inlineCallbacks
    def op_get_params(self, content, headers, msg):
        '''
        Message interface to get parameters
        '''
        logging.info(self.__class__.__name__ +'; Calling Get Params;')
        
        yield self.reply_ok(msg,self.params)
        
    @defer.inlineCallbacks
    def op_get_msg_count(self, content, headers, msg):
        '''
        Message interface to get the message count
        '''
        logging.info(self.__class__.__name__ +'; Calling Get Msg Count;')
        
        yield self.reply_ok(msg,{'count':self.receive_cnt})
        

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
        
        if not hasattr(self, 'params'):
            self.params = {}
                
        res = self.ondata(data, notification, timestamp, **self.params)
        
        logging.info(self.__class__.__name__ +"; op_data: Finished data processing")

        if self.msgs_to_send:
            for ind in range(len(self.msgs_to_send)):
                queue, msg = self.msgs_to_send.pop(0)
                yield self.send(queue, 'data', msg) 
    
            logging.info(self.__class__.__name__ +"; op_data: Finished sending results")


    def ondata(self, data, notification,timestamp, **kwargs):
        """
        Override this method
        """
        raise NotImplementedError, "BaseConsumer class does not implement ondata"
        
        
    def queue_result(self,queue, data=None, notification=''):
        
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
        
        self.msgs_to_send.append((queue, msg.encode()))
        
        
        
class ConsumerDesc(ProcessDesc):
    
    def __init__(self, **kwargs):
        
        ProcessDesc.__init__(self,**kwargs)
        
        # Does it make sense to try and keep state in the Desc?
        #self.proc_attached = None
        #self.proc_params = None
    
    @defer.inlineCallbacks
    def attach(self,queue):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'attach', {'queue':queue})
        if content.get('status','ERROR') == 'OK':
            #self.proc_attached = queue
            defer.returnValue('OK')
        else:
            #self.proc_attached = None
            defer.returnValue('ERROR')
        
    @defer.inlineCallbacks
    def set_params(self,params):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'set_params', params)
        if content.get('status','ERROR') == 'OK':
            #self.proc_params = params
            defer.returnValue('OK')
        else:
            #self.proc_params = None
            defer.returnValue('ERROR')
        
    @defer.inlineCallbacks
    def get_params(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'get_params', {})
        if content.pop('status','ERROR') == 'OK':
            defer.returnValue(content)
        else:
            defer.returnValue('ERROR')

    @defer.inlineCallbacks
    def get_msg_count(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'get_msg_count', {})
        if content.pop('status','ERROR') == 'OK':
            defer.returnValue(content.get('count','ERROR'))
        else:
            defer.returnValue('ERROR')

    
        
        
        