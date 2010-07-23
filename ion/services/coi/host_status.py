#!/usr/bin/env python

"""
@file ion/services/coi/host_status.py
@author Brian Fox
@brief service for messaging local host status at intervals
"""

import logging, xmlrpclib, json
logging = logging.getLogger(__name__)
from twisted.internet import defer, task

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
        timings = [0.3]
        clock = task.Clock()

        def foo():
            p = xmlrpclib.ServerProxy('http://localhost:9010')
            s = p.getStatusPrettyPrint()
            print s
            # print json.dumps(s.getStatus(), indent=4)
            print p.system.listMethods()

        lc = task.LoopingCall(foo)
        lc.start(1)
        return defer.succeed(None)        


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
