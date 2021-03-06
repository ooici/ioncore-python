#!/usr/bin/env python

"""
@file ion/services/coi/host_status.py
@author Brian Fox
@brief service for messaging local host status at intervals
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

try:
    import json
except:
    import simplejson as json


from twisted.internet import defer, task
from twisted.web import xmlrpc

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


class HostStatusService(ServiceProcess):
    """
    Host status interface
    """


    # Declaration of service
    declare = ServiceProcess.service_declare(
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
            log.debug('Shutting down host status looping call')
            self.lc.stop()
            return

        log.debug('Starting report query')
        status = yield self.client.callRemote("getStatusString","all")
        log.debug('Received report')
        log.debug(status)

    def isRunning(self):
        return self.lc.running

    def op_config(self, content, headers, msg):
        pass


    @defer.inlineCallbacks
    def op_sendstatus(self, content, headers, msg):
        yield self.reply_ok(msg)


class HostStatusClient(ServiceClient):
    """
    Class for client to sent log message to service
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "host_status"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def logmsg(self, level, msg, sender, logtime):
        yield self._check_init()
        defer.returnValue(0)

# Spawn of the process using the module name
factory = ProcessFactory(HostStatusService)
