#!/usr/bin/env python

"""
@file ion/services/coi/logger.py
@author Michael Meisinger
@brief service backend for logging. Plays nicely with logging package
"""

import logging.config
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

logserv = logging.getLogger('logServer')

class LoggerService(BaseService):
    """Logger service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='logger', version='0.1.0', dependencies=[])

    def slc_init(self):
        pass

    def op_config(self, content, headers, msg):
        pass

    def op_logmsg(self, content, headers, msg):
        level = content.get('level','info')
        logmsg = content.get('msg','#NO MESSAGE#')
        lfrom = headers.get('sender','')
        ltime = content.get('logtime')

        if level=='debug':
            logserv.debug(logmsg)
        elif level=='info':
            logserv.info(logmsg)
        elif level=='warning':
            logserv.warn(logmsg)
        elif level=='error':
            logserv.error(logmsg)
        elif level=='critical':
            logserv.critical(logmsg)
        else:
            logging.error('Invalid log level: '+str(level))

# Spawn of the process using the module name
factory = ProtocolFactory(LoggerService)

"""
from ion.services.coi import logger
spawn(logger)
send(1,{'op':'logmsg','sender':'snd.1','content':{'level':'info','logtime':'120202112','msg':'Test logging'}})
"""
