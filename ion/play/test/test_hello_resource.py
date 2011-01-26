#!/usr/bin/env python

"""
@file ion/play/test/test_hello_resource.py
@test ion.play.hello_resource Example unit tests for sample resource code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

# Not required but used for inspection... The resource client is in the service!
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.play.hello_resource import HelloResourceClient
from ion.test.iontest import IonTestCase

from ion.core.process.process import Process, ProcessClient, ProcessDesc
from ion.core import bootstrap

from ion.core.object import object_utils

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)
update_resource_type = object_utils.create_type_identifier(object_id=10, version=1)

class HelloResourceTest(IonTestCase):
    """
    Testing example hello object service.
    This example shows how it is possible to create and send strongly typed objects
    Each time an object is sent it is assigned a new identifier. The example
    shows how it is possible to move a linked composite from one object to another.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name':'hello1','module':'ion.play.hello_resource','class':'HelloResource'},
        ]

        sup = yield self._spawn_processes(services)
        
        # It is convienent to have a resource client in the test so we can look
        # at the results of the service calls...
        self.rc = ResourceClient(proc=sup)
            
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
        
        # put the references into self...
        self.repo = repo
        self.ab = ab


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_hello_resource(self):
            
        # Create a hello object client
        hc1 = HelloResourceClient(proc=self.test_sup)
        
        log.info('Calling create addressbook resource ')
        # You can send the root of the object or any linked composite part of it.
        res_ref = yield hc1.create_addressbook_resource(self.ab)
        
        log.info('Create returned resource reference:\n%s' % str(res_ref))
        resource = yield self.rc.get_instance(res_ref)
        
        self.assertEqual(resource.title, 'An addressbook object for testing')
        
        
        ### Modify the addressbook object locally and clobber the state of the resource
        self.ab.title = 'Bad addresses'
        self.ab.person[0].name = 'Jabberwocky'
        self.repo.commit('Must commit before ')
        
        # Create an update request object to send to the service
        update_repo, update = self.test_sup.workbench.init_repository(update_resource_type)

        # Set the reference to update
        update.resource_reference = res_ref
        
        # Set the value to give
        update.configuration = self.ab
        result = yield hc1.clobber_addressbook_resource(update)
        
        # Get the updated state...
        resource = yield self.rc.get_instance(res_ref)
        self.assertEqual(resource.title, 'Bad addresses')
        
        ### Modify the addressbook object locally and merge the state of the resource
        owner = self.repo.create_object(person_type)
        owner.name = 'Whaho!'
        owner.id = 13
        owner.email = 'dear@john.com'
        new_person = self.ab.person.add()
        new_person.SetLink(owner)
        self.ab.owner = owner
        
        self.ab.person[0].name = 'Catch me if you can...'
        
        self.repo.commit('Commit the object before moving to another repo...')
        
        # Create an update request object to send to the service
        update_repo, update = self.test_sup.workbench.init_repository(update_resource_type)

        # Set the reference to update
        update.resource_reference = res_ref
        
        # Set the value to give
        update.configuration = self.ab
        result = yield hc1.merge_addressbook_resource(update)
        
        # Get the updated state...
        resource = yield self.rc.get_instance(res_ref)
        self.assertEqual(resource.title, 'Bad addresses')
        
        
        log.info('Tada!')
        
        
        
        