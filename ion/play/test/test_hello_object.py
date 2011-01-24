#!/usr/bin/env python

"""
@file ion/play/test/test_hello_object.py
@test ion.play.hello_object Example unit tests for sample code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.play.hello_object import HelloObjectClient
from ion.test.iontest import IonTestCase

from ion.core.process.process import Process, ProcessClient, ProcessDesc
from ion.core import bootstrap

from ion.core.object import object_utils

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)


class HelloProcessTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_hello(self):
            
        pd1 = {'name':'hello1','module':'ion.play.hello_object','class':'HelloObject'}
            
        proc1 = ProcessDesc(**pd1)
            
        proc1_id = yield self.test_sup.spawn_child(proc1)
            
        # Each 
        repo, ab = self.test_sup.workbench.init_repository(addresslink_type)
        
        ab.person.add()
    
        p = repo.create_object(person_type)
        p.name = 'david'
        p.id = 59
        p.email = 'stringgggg'
        ab.person[0] = p
        
        print 'AdressBook!',ab
        repo.commit('My addresbook test')



    #    
    #    
    #    log.info('Calling hello there with hc(sup2)')
    #    hc1 = HelloObjectClient(proc=sup2,target=proc1_id)
    #    yield hc1.hello("Hi there, hello1")
    #
    #
    #    #log.info('Calling hello there with hc(sup2)')
    #    #hc2 = HelloObjectClient(proc=sup2,target=proc1_id)
    #    #yield hc2.hello("Hi there, hello1")
    #
    #
    #    log.info('Tada!')