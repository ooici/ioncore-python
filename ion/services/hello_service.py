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

import ion.util.procutils as pu
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
receiver = Receiver(__name__)
instance = HelloService()

def receive(content, msg):
    pu.dispatch_message(content, msg, instance)

receiver.handle(receive)

"""
from ion.services import hello_service as h
h.start()
send(1, {'op':'hello','content':'Hello you there!'})
"""
