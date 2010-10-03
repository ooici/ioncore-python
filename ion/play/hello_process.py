#!/usr/bin/env python

"""
@file ion/play/hello_service.py
@author Michael Meisinger
@brief An example service definition that can be used as template.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from ion.core.messaging.receiver import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory, BaseProcess, BaseProcessClient
#from ion.services.base_service import BaseService, BaseServiceClient

class HelloProcess(BaseProcess):
    """
    Example process
    """


    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        log.info('SLC_INIT HelloProcess')

    @defer.inlineCallbacks
    def op_hello(self, content, headers, msg):
        log.info('op_hello: '+str(content))


        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})


class HelloProcessClient(BaseProcessClient):
    """
    This is an exemplar process client that calls the hello process. It
    makes service calls RPC style.
    """

    @defer.inlineCallbacks
    def hello(self, text='Hi there'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('hello', text)
        log.info('Process replied: '+str(content))
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProtocolFactory(HelloProcess)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
