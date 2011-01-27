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


class HelloObjectTest(IonTestCase):
    """
    Testing example hello object service.
    This example shows how it is possible to create and send strongly typed objects
    Each time an object is sent it is assigned a new identifier. The example
    shows how it is possible to move a linked composite from one object to another.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_hello(self):
        services = [
            {'name':'hello1','module':'ion.play.hello_object','class':'HelloObject'},
        ]

        sup = yield self._spawn_processes(services)  
            
        # Each process has a object work bench. In the test we can get a work bench
        # from the test supervisor process.
        repo, ab = self.test_sup.workbench.init_repository(addresslink_type)
        
        ab.title = 'An addressbook object for testing'
        
        # Add a new entry in the list (repeated) persons of the addressbook
        ab.person.add()
    
        # Make a new person object to go in the list
        person = repo.create_object(person_type)
        person.name = 'david'
        person.id = 59
        person.email = 'stringgggg'
        ph = person.phone.add()
        ph.number = '401 789 6224'
        
        # Since ab.person is of type 'link' we need to set the link equal to the
        # person object
        ab.person[0] = person
        
        log.info('AdressBook!' + str(ab))
        # Calling commit is optional - sending will automatically commit...
        #repo.commit('My addresbook test')
        
        log.info('Repository Status: ' + str(repo.status))
        
        # Create a hello object client
        hc1 = HelloObjectClient(proc=self.test_sup)
        
        log.info('Calling hello person...')
        # You can send the root of the object or any linked composite part of it.
        new_person = yield hc1.hello_person(ab.person[0])
        
        # This is a different repository from the one we started with!
        log.info('Nome of the command object:' + person.Repository.repository_key)
        log.info('Nome of the response object:' + new_person.Repository.repository_key)

        log.info('Repository Status: ' + str(repo.status))
        
        
        # Now lets add the new_person from the reply to the addressbook        
        p2 = ab.person.add()
        
        # You can move a linked object from one repository to another when it
        # has been commited. An exception will be raised if you try and move
        # a modified object. Only a linked composite can be moved
        #p2.SetLink(new_person)
        # you can use an assignment operator or the set link command...
        ab.person[1] = new_person
        
        # The address book now has two perosn object, but you only see the links in the log
        log.info('Print the addressbook after adding the respoonse: ' + str(ab))
        log.info('The hash name of the original person: ' + person.MyId)
        log.info('The hash name of the new_person: ' + new_person.MyId)
        
        
        # Lets try sending the addressbook now...
        # Rather than ssending just one of its person objects, send the whole thing
        yield hc1.hello_everyone(ab)
        
        
        log.info('Tada!')
        
        
        
        