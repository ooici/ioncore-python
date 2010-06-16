#!/usr/bin/env python

"""
@file ion/data/backends/store_service.py
@author David Stuebe
@brief An example service definition that can be used as template.
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.data import store

class HelloService(BaseService):
    """
    Example service interface
    """
    # Declaration of service
    declare = BaseService.service_declare(name='hello',
                                          version='0.1.0',
                                          dependencies=[])

    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('HelloService.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_hello(self, content, headers, msg):
        logging.info('op_hello: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})


class HelloServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def hello(self, text='Hi there'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('hello', text)
        logging.info('Service reply: '+str(content))
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProtocolFactory(HelloService)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
