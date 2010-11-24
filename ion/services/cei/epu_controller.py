#!/usr/bin/env python

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor
from twisted.internet.task import LoopingCall

from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory
from ion.core import bootstrap
import ion.util.procutils as pu
from ion.services.cei.epucontroller import ControllerCore
from ion.services.cei.provisioner import ProvisionerClient
from ion.services.cei import cei_events

class EPUControllerService(ServiceProcess):
    """EPU Controller service interface
    """

    declare = ServiceProcess.service_declare(name='epu_controller', version='0.1.0', dependencies=[])

    def slc_init(self):
        self.queue_name_work = self.get_scoped_name("system", self.spawn_args["queue_name_work"])
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event("controller", "init_begin", log, extra=extradict)
        self.worker_queue = {self.queue_name_work:{'name_type':'worker'}}
        self.laterinitialized = False
        reactor.callLater(0, self.later_init)

        engineclass = "ion.services.cei.decisionengine.impls.DefaultEngine"
        if self.spawn_args.has_key("engine_class"):
            engineclass = self.spawn_args["engine_class"]
            log.info("Using configured decision engine: %s" % engineclass)
        else:
            log.info("Using default decision engine: %s" % engineclass)

        self.provisioner_client = ProvisionerClient(self)

        if self.spawn_args.has_key("engine_conf"):
            engine_conf = self.spawn_args["engine_conf"]
        else:
            engine_conf = None
        self.core = ControllerCore(self.provisioner_client, engineclass, conf=engine_conf)

        self.core.begin_controlling()

        #TODO right now the controller regularly polls the provisioner for
        # updates using the query operation. In the future, this call will
        # come from the provisioner's EPU controller and is designed to
        # support multiple provisioner instances taking operations off of
        # a work queue.
        query_sleep_seconds = 5.0
        log.debug('Starting provisioner query loop - %s second interval',
                query_sleep_seconds)
        self.query_loop = LoopingCall(self.provisioner_client.query)
        self.query_loop.start(query_sleep_seconds, now=False)

    @defer.inlineCallbacks
    def later_init(self):
        yield bootstrap.declare_messaging(self.worker_queue)
        self.laterinitialized = True
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event("controller", "init_end", log, extra=extradict)

    def op_sensor_info(self, content, headers, msg):
        if not self.laterinitialized:
            log.error("message got here without the later-init")
        self.core.new_sensor_info(content)
        
    def op_reconfigure(self, content, headers, msg):
        log.info("EPU Controller: reconfigure: '%'" % content)
        self.core.run_reconfigure(content)

    def op_cei_test(self, content, headers, msg):
        log.info('EPU Controller: CEI test'+ content)

# Direct start of the service as a process with its default name
factory = ProcessFactory(EPUControllerService)
