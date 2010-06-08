#!/usr/bin/env python

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver
from ion.services.base_service import BaseService
from ion.core.base_process import ProtocolFactory
from ion.services.cei.epucontroller import ControllerCore

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class EPUControllerService(BaseService):
    """EPU Controller service interface
    """
    
    declare = BaseService.service_declare(name='epu_controller', version='0.1.0', dependencies=[])
    
    def __init__(self):
        self.provisioner_client = ProvisionerClient(self)
        
    def slc_init(self):
        # todo: make this class configurable
        engineclass = "ion.services.cei.decisionengine.default.DefaultEngine"
        self.core = ControllerCore(self.provisioner_client, engineclass)
        
        self.core.begin_controlling()

    @defer.inlineCallbacks
    def op_sensor_aggregator_info(self, content, headers, msg):
        self.core.new_sensor_info(content)

class ProvisionerClient(object):
    """Abstraction for sending messages to the provisioner.
    """

    def __init__(self, process):
        self.process = process

    @defer.inlineCallbacks
    def provision(self, launch_id, launch_description):
        logging.debug("Sending provision request, id '%s', content '%s'" % (launch_id, launch_description))
        
        # TODO
        #yield self.process.send(sub, operation, record)

# Direct start of the service as a process with its default name
factory = ProtocolFactory(EPUControllerService)
