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

from pydap.model import DatasetType

import numpy

from ion.services.dm.util import dap_tools


class BaseConsumer(BaseProcess):

    

    @defer.inlineCallbacks
    def plc_init(self):
        self.params = self.spawn_args.get('Process Parameters',{})
        self.receive_cnt = {}
        self.received_msg = []
        self.msgs_to_send = []
        self.send_cnt = {}
        self.dataReceivers = {}
        
        
        queuenames = self.spawn_args.get('attach',None)
        if queuenames:
            
            if not hasattr(queuenames,'__iter__'):
                queuenames = [queuenames]
                
            for queue in queuenames:
                res = yield self.attach(queue)
        
            if not res:
                #@Todo - raise an error here?
                logging.info('Failed to attach process to Queue %s in plc_init' % queuename)



    @defer.inlineCallbacks
    def attach(self, queue):
        #@Note - I tried to put a try/except here, but it did not catch the error from magnet
        
        # Check and make sure it is not already attached?
        
        dataReceiver = Receiver(__name__, queue)
        dataReceiver.handle(self.receive)
        dr_id = yield spawn(dataReceiver)
        
        #print dr_id, dataReceiver.name
        
        self.dataReceivers[dataReceiver.name] = dataReceiver
        
        self.receive_cnt[queue]=0
        logging.info("DataConsumer.attach "+str(dr_id)+" to topic "+str(queue))
        defer.returnValue(dr_id)
        
    @defer.inlineCallbacks
    def op_attach(self, content, headers, msg):
        '''
        Message interface to attach to another queue
        '''
        logging.info(self.__class__.__name__ +'; Calling Attach; Queues:' + str(content))
        queues = content.get('queues',None)
        if not queues:
            yield self.reply_err(msg)
            return
        
        if not hasattr(queues,'__iter__'):
            queues = [queues]
        
        for queue in queues:
            id = yield self.attach(queue)
        
        if id:
            yield self.reply_ok(msg)
        else:
            yield self.reply_err(msg)
    '''
    Magnet does not yet support Deattach
    @defer.inlineCallbacks
    def deattach(self, queue):
        #@Note - I tried to put a try/except here, but it did not catch the error from magnet
        
        # Check and make sure it is not already attached?
        del self.dataReceivers[queue]
        logging.info("DataConsumer.deattach; Deattached Queue:"+str(queue))
        return defer.returnValued('OK')

        
    @defer.inlineCallbacks
    def op_deattach(self, content, headers, msg):
        """
        Message interface to attach to another queue
        """
        logging.info(self.__class__.__name__ +'; Calling Deattach; Queues:' + str(content))
        queues = content.get('queues',None)
        if not queues:
            yield self.reply_err(msg)
            return
        
        if not hasattr(queues,'__iter__'):
            queues = [queues]
        
        for queue in queues:
            res = yield self.deattach(queue)
            
        if res:
            yield self.reply_ok(msg)
        else:
            yield self.reply_err(msg)
    '''
        
        
    @defer.inlineCallbacks
    def op_set_process_parameters(self, content, headers, msg):
        '''
        Message interface to set parameters
        '''
        logging.info(self.__class__.__name__ +'; Calling Set Process Parameters; Prameters:' + str(content))
        if not content:
            yield self.reply_err(msg)
            return
        self.params = content
        yield self.reply_ok(msg)
        
    @defer.inlineCallbacks
    def op_get_process_parameters(self, content, headers, msg):
        '''
        Message interface to get parameters
        '''
        logging.info(self.__class__.__name__ +'; Calling Get Process Parameters;')
        
        yield self.reply_ok(msg,self.params)
        
    @defer.inlineCallbacks
    def op_get_msg_count(self, content, headers, msg):
        '''
        Message interface to get the message count
        '''
        logging.info(self.__class__.__name__ +'; Calling Get Msg Count;')
        
        yield self.reply_ok(msg,{'received':self.receive_cnt,'sent':self.send_cnt})
        

    @defer.inlineCallbacks
    def op_data(self, content, headers, msg):

        logging.debug(self.__class__.__name__ +', MSG Received: ' + str(headers))

        logging.info(self.__class__.__name__ + '; Calling data process!')

        # Keep a record of messages received
        #@Note this could get big! What todo?
                
        self.receive_cnt[headers.get('receiver')] += 1
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


        yield defer.maybeDeferred(self.ondata, data, notification, timestamp, **self.params)

        logging.info(self.__class__.__name__ +"; op_data: Finished data processing")

        # Send data only when the process is complete!
        if self.msgs_to_send:
            for ind in range(len(self.msgs_to_send)):
                queue, msg = self.msgs_to_send.pop(0)
                yield self.send(queue, 'data', msg)
                if self.send_cnt.has_key(queue):
                    self.send_cnt[queue] += 1
                else:
                    self.send_cnt[queue] = 1
                    
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
    def attach(self,queues):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'attach', {'queues':queues})
        if content.get('status','ERROR') == 'OK':
            #self.proc_attached = queue
            defer.returnValue('OK')
        else:
            #self.proc_attached = None
            defer.returnValue('ERROR')
        
    '''
    Magnet does not yet support Deattach
    @defer.inlineCallbacks
    def deattach(self,queues):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'deattach', {'queues':queues})
        if content.get('status','ERROR') == 'OK':
            #self.proc_attached = queue
            defer.returnValue('OK')
        else:
            #self.proc_attached = None
            defer.returnValue('ERROR')
    '''
    
    @defer.inlineCallbacks
    def set_process_parameters(self,params):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'set_process_parameters', params)
        if content.get('status','ERROR') == 'OK':
            #self.proc_params = params
            defer.returnValue('OK')
        else:
            #self.proc_params = None
            defer.returnValue('ERROR')
        
    @defer.inlineCallbacks
    def get_process_parameters(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'get_process_parameters', {})
        if content.pop('status','ERROR') == 'OK':
            defer.returnValue(content)
        else:
            defer.returnValue('ERROR')

    @defer.inlineCallbacks
    def get_msg_count(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'get_msg_count', {})
        if content.pop('status','ERROR') == 'OK':
            #defer.returnValue(content.get('count','ERROR'))
            defer.returnValue(content)
        else:
            defer.returnValue('ERROR')

