import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import random
from twisted.internet import defer
from twisted.internet.task import LoopingCall
from twisted.web import server, resource
from twisted.internet import reactor
from ion.core.cc.spawnable import Receiver
from ion.services.base_service import BaseService, BaseServiceClient
from ion.core.base_process import ProtocolFactory
import Queue
import uuid
from ion.services.cei import cei_events

class EPUWorkProducer(BaseService):
    """EPU Work Producer.
    """
    declare = BaseService.service_declare(name='epu_work_producer', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.queue_name_work = self.get_scoped_name("system", self.spawn_args["queue_name_work"])
        self.web_resource = Sidechannel()
        reactor.listenTCP(8000, server.Site(self.web_resource))
        self.work_produce_loop = LoopingCall(self.work_seek)
        self.work_produce_loop.start(1, now=False)
        self.queue_length = 0
        self.last_quelen_send = 0
        self.epu_controller = self.get_scoped_name("system", "epu_controller")

    @defer.inlineCallbacks
    def work_seek(self):
        try:
            while True:
                job = self.web_resource.queue.get(block=False)
                if job == None:
                    raise Queue.Empty()

                yield self.send(self.queue_name_work, 'work', {"work_amount":job.length, "batchid":job.batchid, "jobid":job.jobid})

                self.queue_length += 1

                extradict = {"batchid":job.batchid,
                             "jobid":job.jobid,
                             "work_amount":job.length}
                cei_events.event("workproducer", "job_sent",
                                 logging, extra=extradict)

        except Queue.Empty:
            if self.queue_length == self.last_quelen_send:
                return

            # simulates an increasing queue while we wait for fix
            content = {"queue_id": self.queue_name_work,
                       "queuelen": self.queue_length}
            self.last_quelen_send = self.queue_length
            yield self.send(self.epu_controller, "sensor_info", content)
            return

# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUWorkProducer)

class SleepJob:
    def __init__(self, jobid, batchid, length):
        self.jobid = jobid
        self.batchid = batchid
        self.length = int(length)

# Sidechannel access to tell service what to do
class Sidechannel(resource.Resource):
    isLeaf = True
    def __init__(self):
        self.queue = Queue.Queue()

    def render_GET(self, request):
        parts = request.postpath
        if parts[-1] == "":
            parts = parts[:-1]
        if len(parts) != 4:
            request.setResponseCode(500, "expecting four 'args', /batchid/jobidx/#jobs/#sleepsecs")
            return

        try:
            batchid = parts[0]
            jobidx = int(parts[1])
            jobnum = int(parts[2])
            secperjob = int(parts[3])
        except Exception,e:
            request.setResponseCode(500, "expecting four args, /batchid/jobidx/#jobs/#sleepsecs, those should be ints (except batch id): %s" % e)
            return

        sleepjobs = []
        for i in range(jobnum):
            jobid = i + jobidx
            sleepjobs.append(SleepJob(jobid, batchid, secperjob))

        for job in sleepjobs:
            self.queue.put(job)

        log.debug("enqueued %d jobs with %d sleep seconds" % (jobnum, secperjob))
        return "<html>Success.</html>\n"
