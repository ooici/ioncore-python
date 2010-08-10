#!/usr/bin/env python

"""
@file ion/services/coi/host_status.py
@author Brian Fox
@brief service for messaging local host status at intervals
"""

import logging, json
logging = logging.getLogger(__name__)
from twisted.internet import defer, task
from twisted.web import xmlrpc

from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient


class HostStatusService(BaseService):
    """
    Host status interface
    """


    # Declaration of service
    declare = BaseService.service_declare(
        name='host_status',
        version='0.1.0',
        dependencies=[]
    )


    
    def slc_init(self):
        self.INTERVAL = 1 # seconds
        self.COUNT    = 1

        self.count = self.COUNT
        self.client = xmlrpc.Proxy('http://localhost:9010')
        self.lc = task.LoopingCall(self.report)
        self.lc.start(self.INTERVAL)


    @defer.inlineCallbacks
    def report(self):
        self.count -= 1
        if self.count < 0:
            logging.debug('Shutting down host status looping call')
            self.lc.stop()
            return
            
        logging.debug('Starting report query')
        status = yield self.client.callRemote("getStatusString","all")
        logging.debug('Received report')
        print status
    
    def isRunning(self):
        return self.lc.running
    
    def op_config(self, content, headers, msg):
        pass


    @defer.inlineCallbacks
    def op_sendstatus(self, content, headers, msg):
        yield self.reply_ok(msg)


class HostStatusClient(BaseServiceClient):
    """
    Class for client to sent log message to service
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "host_status"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def logmsg(self, level, msg, sender, logtime):
        yield self._check_init()
        defer.returnValue(0)

# Spawn of the process using the module name
factory = ProtocolFactory(HostStatusService)

"""
from ion.services.coi import logger
spawn(logger)
"""
