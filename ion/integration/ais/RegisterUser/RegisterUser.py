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
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       OOI_ID_TYPE
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
   optional string life_cycle_state=6;
}
"""""

USER_OOIID_TYPE = object_utils.create_type_identifier(object_id=1403, version=1)
"""
from ion-object-definitions/net/ooici/services/coi/identity/identity_management.proto
message UserOoiId {
   enum _MessageTypeIdentifier {
       _ID = 1403;
       _VERSION = 1;
   }

   // objects in a protofile are called messages

   optional string ooi_id=1;
}
"""

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
      
      # build the Identity Registry request for get_user message
      Request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
      Request.configuration = Request.CreateObject(USER_OOIID_TYPE)
      Request.configuration.ooi_id = msg.message_parameters_reference.user_ooi_id
      
      # get the user information from the Identity Registry 
      try:
         user_info = yield self.irc.get_user(Request)
      except ReceivedApplicationError, ex:
         self.fail("get_user failed to find the user [%s]"%msg.message_parameters_reference.user_ooi_id)
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS updateUserDispatcherQueue error response')
         Response.error_num =  ex.msg_content.MessageResponseCode
         Response.error_str =  ex.msg_content.MessageResponseBody
         defer.returnValue(Response)
         
      # build the Identity Registry request for update_user message
      Request.configuration = Request.CreateObject(IDENTITY_TYPE)
      Request.configuration.subject = user_info.resource_reference.subject
      Request.configuration.dispatcher_queue = msg.message_parameters_reference.queue_name
      
      # update the dispatcher queue name for the user  
      try:
         result = yield self.irc.update_user(Request)
      except ReceivedApplicationError, ex:
         self.fail("update_user failed [%s]"%msg.message_parameters_reference.user_ooi_id)
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS updateUserDispatcherQueue error response')
         Response.error_num =  ex.msg_content.MessageResponseCode
         Response.error_str =  ex.msg_content.MessageResponseBody
         defer.returnValue(Response)
         
      # build AIS response
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS updateUserDispatcherQueue response')
      Response.result = Response.ResponseCodes.OK
      defer.returnValue(Response)


   @defer.inlineCallbacks
   def updateUserEmail (self, msg):
      log.info('RegisterUser.updateUserEmail()\n'+str(msg))

      # build the Identity Registry request for get_user message
      Request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
      Request.configuration = Request.CreateObject(USER_OOIID_TYPE)
      Request.configuration.ooi_id = msg.message_parameters_reference.user_ooi_id
      
      # get the user information from the Identity Registry 
      try:
         user_info = yield self.irc.get_user(Request)
      except ReceivedApplicationError, ex:
         self.fail("get_user failed to find the user [%s]"%msg.message_parameters_reference.user_ooi_id)
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS updateUserEmail error response')
         Response.error_num =  ex.msg_content.MessageResponseCode
         Response.error_str =  ex.msg_content.MessageResponseBody
         defer.returnValue(Response)
         
      # build the Identity Registry request for update_user message
      Request.configuration = Request.CreateObject(IDENTITY_TYPE)
      Request.configuration.subject = user_info.resource_reference.subject
      Request.configuration.email = msg.message_parameters_reference.email_address
      
      # update the email address for the user  
      try:
         result = yield self.irc.update_user(Request)
      except ReceivedApplicationError, ex:
         self.fail("update_user failed [%s]"%msg.message_parameters_reference.user_ooi_id)
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS updateUserEmail error response')
         Response.error_num =  ex.msg_content.MessageResponseCode
         Response.error_str =  ex.msg_content.MessageResponseBody
         defer.returnValue(Response)
         
      # build AIS response
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS updateUserEmail response')
      Response.result = Response.ResponseCodes.OK
      defer.returnValue(Response)
 

   @defer.inlineCallbacks
   def registerUser (self, msg):
      log.debug('RegisterUser.registerUser()\n'+str(msg))
      
      # build Identity Registry request for authenticate_user message
      Request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
      Request.configuration = Request.CreateObject(IDENTITY_TYPE)
      Request.configuration.certificate = msg.message_parameters_reference.certificate
      Request.configuration.rsa_private_key = msg.message_parameters_reference.rsa_private_key
      
      # use authenticate_user to try to update a possibly already existing user 
      try:
         result = yield self.irc.authenticate_user(Request)
         log.info('RegisterUser.registerUser(): user exists in IR with ooi_id = '+str(result))
      except ReceivedApplicationError, ex:
            log.info("RegisterUser.registerUser(): calling irc.register_user with\n"+str(Request.configuration))
            # user wasn't in Identity Registry, so register them now
            try:
               result = yield self.irc.register_user(Request)
               log.info('RegisterUser.registerUser(): added new user in IR with ooi_id = '+str(result))
            except ReceivedApplicationError, ex:
               log.info('RegisterUser.registerUser(): Error invoking Identity Registry Service: %s' %ex)
               # build AIS error response
               Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS RegisterUser error response')
               Response.error_num =  ex.msg_content.MessageResponseCode
               Response.error_str =  ex.msg_content.MessageResponseBody
               defer.returnValue(Response)

      # build AIS response with user's ooi_id
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS RegisterUser response')
      Response.message_parameters_reference.add()
      Response.message_parameters_reference[0] = Response.CreateObject(OOI_ID_TYPE)
      Response.message_parameters_reference[0].ooi_id = result.resource_reference.ooi_id
      Response.result = Response.ResponseCodes.OK
      defer.returnValue(Response)
      