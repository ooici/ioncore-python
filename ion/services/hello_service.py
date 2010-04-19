#!/usr/bin/env python

"""
@file ion/services/hello_service.py
@author Michael Meisinger
@brief  example service definition that can be used as template
"""

import logging

from twisted.python import log
from twisted.internet import defer

from magnet.spawnable import Receiver
from magnet.spawnable import send
from magnet.spawnable import spawn
from magnet.store import Store

from ion.services.base_service import BaseService, BaseServiceClient
from ion.services.base_svcproc import BaseServiceProcess

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class HelloService(BaseService):
    """Example service implementation
    """
    
    def __init__(self):
        BaseService.__init__(self)
        logging.info('HelloService.__init__()')

    def slc_init(self):
        pass
    
    def op_hello(self, content, headers, msg):
        logging.info('op_hello: '+str(content))


class HelloServiceClient(BaseServiceClient):
    """Example service client
    """

    def hello():
        pass
    
    def helloAgain():
        pass


# Direct start of the service as a process with its default name
receiver = None

@defer.inlineCallbacks
def start():
    procInst = BaseServiceProcess(__name__, __name__, 'HelloService')
    procInst.receiver.handle(procInst.receive)
    receiver = procInst.receiver
    id = yield spawn(receiver)
    
def receive(content, msg):
    svcProc.receive(content, msg)

"""
from ion.services import hello_service as h
h.start()
send(1, {'op':'hello','content':'Hello you there!'})
"""
