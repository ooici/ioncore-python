#!/usr/bin/env python

"""
@file ion/play/hello_object.py
@author David Stuebe
@brief An example process definition that can be used as template for object communication.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory, Process, ProcessClient
#from ion.core.process.service_process import ServiceProcess, ServiceClient

from net.ooici.play import addressbook_pb2


class HelloObject(Process):
    """
    Example process sends objects
    """

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        log.info('SLC_INIT HelloProcess')

    @defer.inlineCallbacks
    def op_hello(self, content, headers, msg):
        log.info('op_hello: '+str(content))

        ab = content
        
        print 'dir ab', dir(ab)
        
        p = ab.person.add()
        p.name = 'John'
        p.id = 109
        p.email = 'john@doe.com'
        
        print 'AB', ab
        
        # The following line shows how to reply to a message
        yield self.reply(msg, 'result', ab)


class HelloObjectClient(ProcessClient):
    """
    This is an exemplar process client that calls the hello process. It
    makes service calls RPC style.
    """

    @defer.inlineCallbacks
    def hello(self, text='Hi there'):
        yield self._check_init()
        
        repo, ab = self.proc.workbench.init_repository(addressbook_pb2.AddressBook)
        p = ab.person.add()
        p.name = 'david'
        p.id = 59
        p.email = 'stringgggg'
        
        print 'AdressBook!',ab
        #repo.commit('My addresbook test')

        
        (content, headers, msg) = yield self.rpc_send('hello', ab)
        log.info('Process replied: '+str(content))
        defer.returnValue(str(content))

# Spawn of the process using the module name
factory = ProcessFactory(HelloObject)



"""
from ion.play import hello_service as h
spawn(h)
send(1, {'op':'hello','content':'Hello you there!'})

from ion.play.hello_service import HelloServiceClient
hc = HelloServiceClient(1)
hc.hello()
"""
