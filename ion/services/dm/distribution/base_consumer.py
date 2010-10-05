#!/usr/bin/env python


"""
@file ion/services/dm/distribution/base_consumer.py
@author David Stuebe
@brief the base class for pubsub consumer processes
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import time

from twisted.internet import defer
from twisted.internet.task import LoopingCall
from twisted.internet import reactor


#from ion.core.cc.container import Container
from ion.core.messaging.receiver import Receiver, FanoutReceiver

from ion.core.base_process import BaseProcess, ProcessDesc
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import PubSubTopicResource, \
    DAPMessageObject, DataMessageObject, StringMessageObject, DictionaryMessageObject

from ion.services.dm.util import dap_tools

from pydap.model import DatasetType

#import numpy

from ion.services.dm.util import dap_tools


class BaseConsumer(BaseProcess):
    '''
    @brief This is the base class from which all consumer processes should inherit.
    All tranformaitons and data presentation methods should inherit for this
    and implement the ondata method to perform the desired task.
    '''

    @defer.inlineCallbacks
    def plc_init(self):
        p = self.spawn_args.get('process parameters',{})
        self.params = {}
        for k,v in p.items():
            self.params[str(k)] = v

        d = self.spawn_args.get('delivery queues',{})
        self.deliver = {}
        for k,v in d.items():
            self.deliver[str(k)] = v

        # Scheduled interval delivery - digest mode!
        self.delivery_interval = self.spawn_args.get('delivery interval',None)
        if self.delivery_interval:
            assert isinstance(self.delivery_interval, (int,float)), 'delivery interval must be a float or a integer'
        self.last_delivered = None
        self.interval_cnt = {}
        self.loop = None
        #if self.delivery_interval:
        #    self.loop = LoopingCall(self.digest)
        self.loop_running = False


        self.receive_cnt = {}
        #self.received_msg = []
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
                    #@todo - raise an error here?
                    log.info('Failed to attach process to Queue %s in plc_init' % queuename)


        # Run any custom initialization provided by this consumer
        log.debug(self.__class__.__name__ + ' running customize_consumer')
        yield defer.maybeDeferred(self.customize_consumer)
        log.debug(self.__class__.__name__ + ' customize_consumer complete!')


    def customize_consumer(self):
        '''
        Use this method to customize the initialization of the consumer
        '''

    @defer.inlineCallbacks
    def attach(self, queue):
        #@note - I tried to put a try/except here, but it did not catch the error from CC

        # Check and make sure it is not already attached?
        dataReceiver = FanoutReceiver(label=__name__,
                                        name=str(queue),
                                        handler=self.receive)
        dr_id = yield dataReceiver.attach()

        #print dr_id, dataReceiver.name

        self.dataReceivers[dataReceiver.name] = dataReceiver

        self.receive_cnt[queue]=0
        log.info("DataConsumer.attach "+str(dr_id)+" to topic "+str(queue))
        defer.returnValue(dr_id)

    @defer.inlineCallbacks
    def op_attach(self, content, headers, msg):
        '''
        Message interface to attach to another queue
        '''
        log.info(self.__class__.__name__ +'; Calling Attach; Queues:' + str(content))
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
        #@note - I tried to put a try/except here, but it did not catch the error from CC

        # Check and make sure it is not already attached?
        del self.dataReceivers[queue]
        log.info("DataConsumer.deattach; Deattached Queue:"+str(queue))
        return defer.returnValued('OK')


    @defer.inlineCallbacks
    def op_deattach(self, content, headers, msg):
        """
        Message interface to attach to another queue
        """
        log.info(self.__class__.__name__ +'; Calling Deattach; Queues:' + str(content))
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
        log.info(self.__class__.__name__ +'; Calling Set Process Parameters; Prameters:' + str(content))
        if not content:
            yield self.reply_err(msg)
            return
        self.params.update(content)
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_get_process_parameters(self, content, headers, msg):
        '''
        Message interface to get parameters
        '''
        log.info(self.__class__.__name__ +'; Calling Get Process Parameters;')

        yield self.reply_ok(msg,self.params)

    @defer.inlineCallbacks
    def op_set_delivery_queues(self, content, headers, msg):
        '''
        Message interface to set parameters
        '''
        log.info(self.__class__.__name__ +'; Calling Set Delivery Queues; Queues:' + str(content))
        if not content:
            yield self.reply_err(msg)
            return
        self.params.update(content)
        yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_get_delivery_queues(self, content, headers, msg):
        '''
        Message interface to get parameters
        '''
        log.info(self.__class__.__name__ +'; Calling Get Delivery Queues;')

        yield self.reply_ok(msg,self.deliver)

    @defer.inlineCallbacks
    def op_get_msg_count(self, content, headers, msg):
        '''
        Message interface to get the message count
        '''
        log.info(self.__class__.__name__ +'; Calling Get Msg Count;')

        yield self.reply_ok(msg,{'received':self.receive_cnt,'sent':self.send_cnt})

    @defer.inlineCallbacks
    def op_data(self, content, headers, msg):

        log.debug(self.__class__.__name__ +', MSG Received: ' + str(headers))

        log.info(self.__class__.__name__ + '; Calling data process!')

        # Keep a record of messages received
        #@note this could get big! What todo?

        self.receive_cnt[headers.get('receiver')] += 1
        #self.received_msg.append(content) # Do not keep the messages!

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

        # Build the keyword args for ondata
        args = dict(self.params)
        args.update(self.deliver)

        log.debug('**ARGS to ondata:'+str(args))
        yield defer.maybeDeferred(self.ondata, data, notification, timestamp, **args)

        log.info(self.__class__.__name__ +"; op_data: Finished data processing")


        # Is this a consumer with digest delivery?
        if not self.delivery_interval:
            # if not send the messages from ondata...
            yield self.deliver_messages()
            log.info(self.__class__.__name__ +"; op_data: Finished sending results")

        else: # Do the digets thing...

            if self.interval_cnt.has_key(headers.get('receiver')):
                self.interval_cnt[headers.get('receiver')] += 1
            else:
                self.interval_cnt[headers.get('receiver')] = 1


            log.debug(self.__class__.__name__ +"; op_data: digest state: \n" + \
                          "Last Delivered: " +str(self.last_delivered) +";\n" +\
                          "Loop Running: " +str(self.loop_running))

            # First time data has arrived?
            if self.last_delivered == None:
                self.last_delivered = pu.currenttime()

            if not self.loop_running:

                # Is it already time to go?
                if self.last_delivered + self.delivery_interval <= pu.currenttime():
                    yield self.digest()

                # if data has arrived but it is not yet time to deliver, schedule a call back
                else:
                    self.loop_running = True
                    delta_t = self.last_delivered + self.delivery_interval - pu.currenttime()
                    log.debug('Scheduling a call back in %s seconds' % delta_t)
                    #self.loop.start(delta_t)
                    reactor.callLater(delta_t, self.digest)

                    #IReactorTime.callLater(delta_t, self.digest)



    def ondata(self, data, notification,timestamp, **kwargs):
        """
        Override this method
        """
        raise NotImplementedError, "BaseConsumer class does not implement ondata"

    @defer.inlineCallbacks
    def digest(self):

        log.info(self.__class__.__name__ +"; Digesting results!")

        # Stop the loop if it is running - start again when next data is received
        if self.loop_running:
            #self.loop.stop()
            self.loop_running = False

        args = dict(self.params)
        args.update(self.deliver)

        yield defer.maybeDeferred(self.onschedule, **args)

        yield self.deliver_messages()
        # Update last_delivered
        self.interval_cnt={} # Reset the interval receive count
        self.last_delivered = pu.currenttime()
        log.info(self.__class__.__name__ +"; digest: Finished sending results")

    def onschedule(self, intrval_cnt, **kwargs):
        """
        Override this method
        """
        raise NotImplementedError, "BaseConsumer class does not implement onschedule"


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

    @defer.inlineCallbacks
    def deliver_messages(self):
        # Send data only when the process is complete!
        if self.msgs_to_send:
            while len(self.msgs_to_send) > 0:
                queue, msg = self.msgs_to_send.pop(0)
                yield self.send(queue, 'data', msg)
                if self.send_cnt.has_key(queue):
                    self.send_cnt[queue] += 1
                else:
                    self.send_cnt[queue] = 1

class ConsumerDesc(ProcessDesc):
    '''
    @brief The ConsumerDesc class inherits from the ProcessDesc class and is used
    to create and control consumer processes.
    '''


    def __init__(self, **kwargs):

        ProcessDesc.__init__(self,**kwargs)

        # Does it make sense to try and keep state in the Desc?
        #self.proc_attached = None
        #self.proc_params = None

    @defer.inlineCallbacks
    def attach(self,queues):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'attach', {'queues':queues})
        if headers.get('status','ERROR') == 'OK':
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
        if headers.get('status','ERROR') == 'OK':
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
        if headers.get('status','ERROR') == 'OK':
            #self.proc_params = params
            defer.returnValue('OK')
        else:
            #self.proc_params = None
            defer.returnValue('ERROR')

    @defer.inlineCallbacks
    def get_process_parameters(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'get_process_parameters', {})
        if headers.get('status','ERROR') == 'OK':
            defer.returnValue(content)
        else:
            defer.returnValue('ERROR')

    @defer.inlineCallbacks
    def set_delivery_queues(self,params):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'set_delivery_queues', params)
        if headers.get('status','ERROR') == 'OK':
            #self.proc_params = params
            defer.returnValue('OK')
        else:
            #self.proc_params = None
            defer.returnValue('ERROR')

    @defer.inlineCallbacks
    def get_delivery_queues(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'get_delivery_queues', {})
        if headers.get('status','ERROR') == 'OK':
            defer.returnValue(content)
        else:
            defer.returnValue('ERROR')

    @defer.inlineCallbacks
    def get_msg_count(self):
        (content, headers, msg) = yield self.sup_process.rpc_send(self.proc_id,
                                                'get_msg_count', {})
        if headers.get('status','ERROR') == 'OK':
            #defer.returnValue(content.get('count','ERROR'))
            defer.returnValue(content)
        else:
            defer.returnValue('ERROR')
