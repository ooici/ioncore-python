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
        log.info('op_hello: ')

        ab = content
                
        p = ab.person[0]
        
        assert p.name == 'david', 'Not reading right!'
        
        print 'Print Test'
        for person in ab.person:
            print 'person',person
        print 'owner',ab.owner
        
        
        
        ab.person.add()
        p = ab.repository.create_wrapped_object(addressbook_pb2.Person)
        p.name = 'John'
        p.id = 109
        p.email = 'john@doe.com'
        
        ab.person[1] = p
        
        ab.owner = p
        
        print 'AB', ab
        
        print 'Commiting.....'
        
        ab.repository.commit('adding a commit')
        
        print 'Commited, now sending reply'
        
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
        
        repo, ab = self.proc.workbench.init_repository(addressbook_pb2.AddressLink)
        
        ab.person.add()

        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name = 'david'
        p.id = 59
        p.email = 'stringgggg'
        ab.person[0] = p
        
        print 'AdressBook!',ab
        repo.commit('My addresbook test')

        
        (content, headers, msg) = yield self.rpc_send('hello', ab)
        log.info('Process replied: ')
        
        for key,item in content.repository._workspace.items():
            print 'WORKSPACE,',key,item
        
        for person in content.person:
            print 'person',person
        print 'owner',content.owner
        
        #print 'LinkClassType',ab.LinkClassType
        
        defer.returnValue(content)

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
