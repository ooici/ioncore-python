#!/usr/bin/env python

import logging
import random
from twisted.internet import defer
from twisted.internet.task import LoopingCall
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService, BaseServiceClient
from ion.core.base_process import ProtocolFactory

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUWorkProducer(BaseService):
    """EPU Work Producer.
    """
    declare = BaseService.service_declare(name='epu_work_producer', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.queue_name_work = self.spawn_args["queue_name_work"]
        self.work_produce_loop = LoopingCall(self.work_produce)
        self.work_produce_loop.start(1) #XXX temporary for testing

    def op_start_producing(self, content, headers, msg):
        logging.info("op_start_producing")
        work_produce_interval = content["work_produce_interval"]
        self.work_produce_loop.start(work_produce_interval)

    @defer.inlineCallbacks
    def work_produce(self): 
        work_amount = random.randint(1, 10) # "work", aka sleep for given seconds
        yield self.send(self.queue_name_work, 'work', {"work_amount":work_amount})


class EPUWorkProducerClient(BaseServiceClient):
    """Client for sending the "start_producing" message to
    the EPUWorkProducer to start sending work messages to the EPU Workers.
    """

    def __init__(self, work_produce_interval=1, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "epu_work_producer"
        BaseServiceClient.__init__(self, proc, **kwargs)
        self.work_produce_interval = work_produce_interval

    @defer.inlineCallbacks
    def start_work_producer(self):
        """Send message to epu_work_producer to make it start producing work"""
        yield self._check_init()
        logging.debug('Commanding epu_work_producer to start producing')
        request = {"work_produce_interval":self.work_produce_interval}
        yield self.send('start_producing', request)
 

# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUWorkProducer)
