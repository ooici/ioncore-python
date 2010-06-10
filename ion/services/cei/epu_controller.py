#!/usr/bin/env python

import logging
from twisted.internet import defer
from twisted.internet.task import LoopingCall
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory
from ion.services.cei.epucontroller import ControllerCore
from ion.services.cei.provisioner import ProvisionerClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUControllerService(BaseService):
    """EPU Controller service interface
    """
    
    declare = BaseService.service_declare(name='epu_controller', version='0.1.0', dependencies=[])
    
    def slc_init(self):
        # todo: make this class configurable
        engineclass = "ion.services.cei.decisionengine.default.DefaultEngine"
        self.provisioner_client = ProvisionerClient(self)
        self.core = ControllerCore(self.provisioner_client, engineclass)
        
        self.core.begin_controlling()

        #TODO right now the controller regularly polls the provisioner for
        # updates using the query operation. In the future, this call will
        # come from the provisioner's EPU controller and is designed to
        # support multiple provisioner instances taking operations off of
        # a work queue.
        query_sleep_seconds = 5.0
        logging.debug('Starting provisioner query loop - %s second interval', 
                query_sleep_seconds)
        self.query_loop = LoopingCall(self.provisioner_client.query)
        self.query_loop.start(query_sleep_seconds)

    @defer.inlineCallbacks
    def op_sensor_info(self, content, headers, msg):
        self.core.new_sensor_info(content)

    def op_cei_test(self, content, headers, msg):
        logging.info('EPU Controller: CEI test worked!!!!11! '+ content)

# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUControllerService)
