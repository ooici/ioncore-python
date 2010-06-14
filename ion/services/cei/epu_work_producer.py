#!/usr/bin/env python

import logging
import random
from twisted.internet import defer
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUWorkProducer(BaseService):
    """EPU Work Producer.
    """
    declare = BaseService.service_declare(name='epu_work_producer', version='0.1.0', dependencies=[])
    work_produce_interval = 1 #in seconds

    def slc_init(self):
        self.queue_name_work = self.spawnArgs["queue_name_work"]
        self.work_produce_loop = LoopingCall(self.work_produce)

    def op_start_producing(self, content, headers, msg):
        logging.info("op_start_producing")
        self.work_produce_loop.start(self.work_produce_interval)

    @defer.inlineCallbacks
    def work_produce(self): 
        work_amount = random.randint(1, 10) # "work", aka sleep for given seconds
        yield self.send(self.queue_name_work, 'work', {"work_amount":work_amount})
 

# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUWorkProducer)
