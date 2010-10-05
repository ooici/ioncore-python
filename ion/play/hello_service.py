#!/usr/bin/env python

"""
@file ion/play/hello_service.py
@author Michael Meisinger
@brief An example service definition that can be used as template.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

class HelloService(ServiceProcess):
    """
    Example service interface
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='hello',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        ServiceProcess.__init__(self, *args, **kwargs)
        log.info('HelloService.__init__()')

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        pass

    @defer.inlineCallbacks
    def op_hello(self, content, headers, msg):
        log.info('op_hello: '+str(content))

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})


class HelloServiceClient(ServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "hello"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def hello(self, text='Hi there'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('hello', text)
        log.info('Service reply: '+str(content))
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProcessFactory(HelloService)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
