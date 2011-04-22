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
from ion.services.coi.resource_registry.resource_client import ResourceClient

from ion.play.hello_resource import HelloResourceClient
from ion.test.iontest import IonTestCase

from ion.core.messaging.message_client import MessageClient


from ion.core.object import object_utils

# from net.ooici.play instrument_example.proto
INSTRUMENT_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=20029, version=1)
"""
package net.ooici.play;


message InstrumentResource{
    enum _MessageTypeIdentifier {
      _ID = 20029;
      _VERSION = 1;
    }
    optional string name = 1;
    optional string make = 2;
    optional string model = 3;
    optional string serial_number = 4;
}

"""
# from net.ooici.play instrument_example.proto

INSTRUMENT_INFO_TYPE = object_utils.create_type_identifier(object_id=20030, version=1)
"""

message InstrumentInfoObject{
    enum _MessageTypeIdentifier {
      _ID = 20030;
      _VERSION = 1;
    }
    optional string name = 1;
    optional string make = 2;
    optional string model = 3;
    optional string serial_number = 4;
}
"""

RESOURCE_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
"""
package net.ooici.core.message;

import "net/ooici/core/link/link.proto";

message ResourceConfigurationRequest{
    enum _MessageTypeIdentifier {
      _ID = 10;
      _VERSION = 1;
    }
    
    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
    
    enum LifeCycleOperation {
	Activate=1;
	Deactivate=2;
	Commission=3;
	Decommission=4;
	Retire=5;
	Develope=6;
    }
    
    optional LifeCycleOperation life_cycle_operation = 3;
    
}

"""

RESOURCE_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)
"""
message ResourceConfigurationResponse{
    enum _MessageTypeIdentifier {
      _ID = 12;
      _VERSION = 1;
    }
    
    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
    
    optional string result = 3;
}
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
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry.resource_registry','class':'ResourceRegistryService',
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
        
        create_request_msg = yield self.mc.create_instance(MessageContentTypeID = RESOURCE_REQUEST_TYPE)
        
        create_request_msg.configuration = create_request_msg.CreateObject(INSTRUMENT_INFO_TYPE)
        
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
        update_request_msg = yield self.mc.create_instance(RESOURCE_REQUEST_TYPE)
        
        # Copy from the create request and make some changes...
        update_request_msg.configuration = create_request_msg.configuration
        update_request_msg.configuration.name = 'Junk'
        update_request_msg.configuration.model = 'Jabberwocky'
        
        # Set the reference to update
        update_request_msg.resource_reference = create_response_msg.resource_reference
        
        # Call the update method 
        update_result_msg = yield hc1.update_instrument_resource(update_request_msg)
        
        
        ### request to update the state of the resource
        lcs_request_msg = yield self.mc.create_instance(RESOURCE_REQUEST_TYPE)

        lcs_request_msg.resource_reference = create_response_msg.resource_reference
        lcs_request_msg.life_cycle_operation = lcs_request_msg.MessageObject.LifeCycleOperation.ACTIVATE
        
        # Call the life cycle operation method method 
        update_result_msg = yield hc1.set_instrument_resource_life_cycle(lcs_request_msg)
        
        log.info('Tada!')
        
        
        
        