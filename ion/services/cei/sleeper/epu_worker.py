#!/usr/bin/env python

import time
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor
from ion.core.messaging.receiver import Receiver
from ion.core import bootstrap
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory
import ion.util.procutils as pu
from ion.services.cei import cei_events

class EPUWorkerService(BaseService):
    """EPU Worker service.
    """
    declare = BaseService.service_declare(name='epu_worker', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.queue_name_work = self.get_scoped_name("system", self.spawn_args["queue_name_work"])
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event("worker", "init_begin", logging, extra=extradict)
        self.workReceiver = Receiver(__name__, self.queue_name_work)
        self.worker_queue = {self.queue_name_work:{'name_type':'worker'}}
        self.laterinitialized = False
        reactor.callLater(0, self.later_init)

    @defer.inlineCallbacks
    def later_init(self):
        yield bootstrap.declare_messaging(self.worker_queue)
        self.workReceiver.add_handler(self.receive)
        spawnId = yield self.workReceiver.activate()
        log.debug("spawnId: %s" % spawnId)
        self.laterinitialized = True
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event("worker", "init_end", logging, extra=extradict)

    @defer.inlineCallbacks
    def op_work(self, content, headers, msg):
        if not self.laterinitialized:
            log.error("message got here without the later-init")
        sleepsecs = int(content['work_amount'])
        extradict = {"batchid":content['batchid'],
                     "jobid":content['jobid'],
                     "work_amount":sleepsecs}
        cei_events.event("worker", "job_begin", logging, extra=extradict)
        log.info("WORK: sleeping for %d seconds ---" % sleepsecs)
        yield pu.asleep(sleepsecs)
        yield self.reply(msg, 'result', {'result':'work_complete'}, {})
        cei_events.event("worker", "job_end", logging, extra=extradict)


# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUWorkerService)
