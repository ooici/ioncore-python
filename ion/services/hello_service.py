#!/usr/bin/env python

"""
@file ion/services/hello_service.py
@author Michael Meisinger
@brief  example service definition that can be used as template
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

from ion.core import ionconst as ic
import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, RpcClient
from ion.services.base_service import BaseService, BaseServiceClient

logging.basicConfig(level=logging.DEBUG)
logging.debug('Loaded: '+__name__)

class HelloService(BaseService):
    """Example service implementation
    """
    
    def __init__(self, receiver, spawnArgs=None):
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('HelloService.__init__()')

    def slc_init(self):
        pass

    @defer.inlineCallbacks
    def op_hello(self, content, headers, msg):
        logging.info('op_hello: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_message(msg, 'reply', {'value':'Hello there, '+str(content)}, {})


class HelloServiceClient(RpcClient):
    """This is an exemplar service class that calls the hello service. It
    applies the RPC pattern.
    """

    @defer.inlineCallbacks
    def hello(self, to='1', text='Hi there'):
        cont = yield self.rpc_send(to, 'hello', text, {})
        logging.info('Friends reply: '+str(cont))


# Spawn of the process using the module name
factory = ProtocolFactory(HelloService)



"""
from ion.services import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.services.hello_service import HelloServiceClient
hc = HelloServiceClient()
hc.attach()
hc.hello()
"""
