#!/usr/bin/env python

import time
import logging
from twisted.internet import defer
from magnet.spawnable import spawn, Receiver
from ion.core import bootstrap
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUWorkerService(BaseService):
    """EPU Worker service.
    """
    declare = BaseService.service_declare(name='epu_worker', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        self.queue_name_work = self.get_scoped_name("system", self.spawn_args["queue_name_work"])
        worker_queue = {self.queue_name_work:{'name_type':'worker'}}
        yield bootstrap.declare_messaging(worker_queue)

        workReceiver = Receiver(__name__, self.queue_name_work)
        self.workReceiver = workReceiver
        self.workReceiver.handle(self.receive)
        spawnId = yield spawn(workReceiver)


    @defer.inlineCallbacks
    def op_work(self, content, headers, msg):
        logging.info("EPUWorkerService ---doing work--- content:"+str(content))
        time.sleep(content['work_amount'])
        yield self.reply(msg, 'result', {'result':'work_complete'}, {})


# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUWorkerService)


