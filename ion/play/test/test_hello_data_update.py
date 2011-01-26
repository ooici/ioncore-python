#!/usr/bin/env python

"""
@file ion/play/test/test_hello_resource.py
@test ion.play.hello_resource Example unit tests for sample resource code.
@author David Stuebe
"""
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.play.hello_data_update import HelloDataUpdateClient
from ion.test.iontest import IonTestCase

from ion.core.process.process import Process, ProcessClient, ProcessDesc
from ion.core import bootstrap

from ion.core.object import object_utils

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)
resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)

class HelloDataUpdateTest(IonTestCase):
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
            {'name':'hello1','module':'ion.play.hello_data_update','class':'HelloDataUpdate'},
        ]

        sup = yield self._spawn_processes(services)
        
        self.mc = MessageClient(self.test_sup)
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_hello_resource(self):
            
        # Create a hello object client
        hc1 = HelloDataUpdateClient(proc=self.test_sup)
        
        create_request_msg = yield self.mc.create_instance(resource_request_type, name='Create me!')
        
        create_request_msg.configuration = create_request_msg.CreateObject(addresslink_type)
        
        create_request_msg.configuration.title = 'An addressbook object for testing'
        
        # Add a new entry in the list (repeated) persons of the addressbook
        create_request_msg.configuration.person.add()
    
        # Make a new person object to go in the list
        create_request_msg.configuration.person[0] = create_request_msg.CreateObject(person_type)

        create_request_msg.configuration.person[0].name = 'david'
        create_request_msg.configuration.person[0].id = 59
        create_request_msg.configuration.person[0].email = 'stringgggg'
        create_request_msg.configuration.person[0].phone.add()
        create_request_msg.configuration.person[0].phone.number = '401 789 6224'
        
        log.info('AdressBook!' + str(create_request_msg))
        
        
        log.info('Calling create addressbook resource ')
        # You can send the root of the object or any linked composite part of it.
        create_response_msg = yield hc1.create_addressbook_resource(create_request_msg)
        
        log.info('Create returned resource reference:\n%s' % str(create_response_msg))
        
        self.assertEqual(create_response_msg.configuration.title, 'An addressbook object for testing')
        
        
        ### request to clobber the state of the resource
        clobber_request_msg = yield self.mc.create_instance(resource_request_type, name='Clobber it!')
        
        # Get the current and make some changes...
        clobber_request_msg.configuration = create_response_msg.configuration
        clobber_request_msg.configuration.title = 'Bad addresses'
        clobber_request_msg.configuration.person[0].name = 'Jabberwocky'
        
        # Set the reference to update
        clobber_request_msg.resource_reference = create_response_msg.resource_reference
        
        
        clobber_result_msg = yield hc1.clobber_addressbook_resource(clobber_request_msg)
        
        
        ### request to merge the state of the resource
        merge_request_msg = yield self.mc.create_instance(resource_request_type, name='merge me!')
        
        merge_request_msg.configuration = create_response_msg.configuration
        
        merge_request_msg.configuration.owner = merge_request_msg.CreateObject(person_type)
        merge_request_msg.configuration.owner.name = 'Whaho!'
        merge_request_msg.configuration.owner.id = 13
        merge_request_msg.configuration.owner.email = 'dear@john.com'
        merge_request_msg.configuration.person.add()
        merge_request_msg.configuration.person[1] = merge_request_msg.configuration.owner
        
        merge_request_msg.configuration.person[0].name = 'Catch me if you can...'
        
        merge_request_msg.resource_reference = create_response_msg.resource_reference
        
        result = yield hc1.merge_addressbook_resource(merge_request_msg)
        
        log.info('Tada!')
        
        
        
        