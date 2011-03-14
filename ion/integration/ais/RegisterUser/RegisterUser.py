#!/usr/bin/env python

"""
@file ion/integration/ais/RegisterUser/RegisterUser.py
@author Bill Bollenbcher
@brief The worker class that implements the RegisterUser function for the AIS  (workflow #107)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, AIS_REQUEST_MSG_TYPE, OOI_ID_TYPE
from ion.core.object import object_utils

IDENTITY_TYPE = object_utils.create_type_identifier(object_id=1401, version=1)
"""
from ion-object-definitions/net/ooici/services/coi/identity/identity_management.proto
message UserIdentity {
   enum _MessageTypeIdentifier {
       _ID = 1401;
       _VERSION = 1;
   }

   // objects in a protofile are called messages

   optional string subject=1;
   optional string certificate=2;
   optional string rsa_private_key=3;
   optional string dispatcher_queue=4
   optional string email=5
}
"""""

RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
message ResourceConfigurationRequest{
    enum _MessageTypeIdentifier {
      _ID = 10;
      _VERSION = 1;
    }
    
    // The identifier for the resource to configure
    optional net.ooici.core.link.CASRef resource_reference = 1;

    // The desired configuration object
    optional net.ooici.core.link.CASRef configuration = 2;
"""

RESOURCE_CFG_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=12, version=1)
"""
from ion-object-definitions/net/ooici/core/message/resource_request.proto
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

class RegisterUser(object):
    
   def __init__(self, ais):
      log.debug('RegisterUser.__init__()')
      self.irc = IdentityRegistryClient(proc=ais)
      self.mc = ais.mc
        
   @defer.inlineCallbacks
   def updateUserDispatcherQueue (self, msg):
      log.info('RegisterUser.updateUserDispatcherQueue()\n'+str(msg))
      result = yield self.irc.update_user(msg)
      defer.returnValue(result)

   @defer.inlineCallbacks
   def updateUserEmail (self, msg):
      log.info('RegisterUser.updateUserEmail()\n'+str(msg))
      result = yield self.irc.update_user(msg)
      defer.returnValue(result)

   @defer.inlineCallbacks
   def registerUser (self, msg):
      log.debug('RegisterUser.registerUser()\n'+str(msg))
      """
      result = yield self.irc.authenticate_user(msg.message_parameters_reference.certificate,
                                                msg.message_parameters_reference.rsa_private_key)
      if type(result) == str:   
         log.info('RegisterUser.registerUser(): user exists in IR with ooi_id = '+str(result))
      else:
      """
      Request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR register_user request')
      Request.configuration = Request.CreateObject(IDENTITY_TYPE)
      Request.configuration.certificate = msg.message_parameters_reference.certificate
      Request.configuration.rsa_private_key = msg.message_parameters_reference.rsa_private_key
      log.info("RegisterUser.registerUser(): calling irc with\n"+str(Request.configuration))
      result = yield self.irc.register_user(Request)
      log.info('RegisterUser.registerUser(): added new user in IR with ooi_id = '+str(result))
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS RegisterUser response')
      Response.message_parameters_reference.add()
      Response.message_parameters_reference[0] = Response.CreateObject(OOI_ID_TYPE)
      Response.message_parameters_reference[0].ooi_id = result
      defer.returnValue(Response)



