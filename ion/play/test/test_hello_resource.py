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

from ion.core.messaging.message_client import MessageClient


from ion.core.process.process import Process, ProcessClient, ProcessDesc
from ion.core import bootstrap

from ion.core.object import object_utils

instrument_resource_type = object_utils.create_type_identifier(object_id=20029, version=1)
instrument_info_type = object_utils.create_type_identifier(object_id=20030, version=1)
resource_request_type = object_utils.create_type_identifier(object_id=10, version=1)
resource_response_type = object_utils.create_type_identifier(object_id=12, version=1)


class HelloResourceTest(IonTestCase):
    """
    Testing example hello object service.
    This example shows how it is possible to create and send strongly typed objects
    Each time an object is sent it is assigned a new identifier. The example
    shows how it is possible to move a linked composite from one object to another.
    """

class HelloResourceTest(IonTestCase):
    """
    Testing example hello resource service.
    This example shows how it is possible to create and send resource requests.
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
        
        self.mc = MessageClient(proc = self.test_sup)
        

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_hello_resource(self):
            
        # Create a hello object client
        hc1 = HelloResourceClient(proc=self.test_sup)
        
        create_request_msg = yield self.mc.create_instance(resource_request_type, MessageName='Create me!')
        
        create_request_msg.configuration = create_request_msg.CreateObject(instrument_info_type)
        
        create_request_msg.configuration.name = 'A CTD'
        create_request_msg.configuration.make = 'SeaBird'
        create_request_msg.configuration.model = 'SBE911'
        create_request_msg.configuration.serial_number = 'ax1d92p'
        
        
        log.info('Instrument config message! \n' + str(create_request_msg))
        
        
        log.info('Calling create instrument resource ')
        # You can send the root of the object or any linked composite part of it.
        create_response_msg = yield hc1.create_instrument_resource(create_request_msg)
        
        log.info('Create returned resource reference:\n%s' % str(create_response_msg))
        
        self.assertEqual(create_response_msg.configuration.name, 'A CTD')
        
        ### request to update the state of the resource
        update_request_msg = yield self.mc.create_instance(resource_request_type, MessageName='Update it!')
        
        # Copy from the create request and make some changes...
        update_request_msg.configuration = create_request_msg.configuration
        update_request_msg.configuration.name = 'Junk'
        update_request_msg.configuration.model = 'Jabberwocky'
        
        # Set the reference to update
        update_request_msg.resource_reference = create_response_msg.resource_reference
        
        # Call the update method 
        update_result_msg = yield hc1.update_instrument_resource(update_request_msg)
        
        
        ### request to update the state of the resource
        lcs_request_msg = yield self.mc.create_instance(resource_request_type, MessageName='Update it!')

        lcs_request_msg.resource_reference = create_response_msg.resource_reference
        lcs_request_msg.life_cycle_operation = lcs_request_msg.MessageObject.LifeCycleOperation.ACTIVATE
        
        # Call the life cycle operation method method 
        update_result_msg = yield hc1.set_instrument_resource_life_cycle(lcs_request_msg)
        
        log.info('Tada!')
        
        
        
        