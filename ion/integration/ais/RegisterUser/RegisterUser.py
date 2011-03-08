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

class RegisterUser(object):
    
   def __init__(self, ais):
      log.info('RegisterUser.__init__()')
      self.irc = IdentityRegistryClient(proc=ais)
      self.mc = ais.mc
        
   @defer.inlineCallbacks
   def updateUserDispatcherQueue (self, msg):
      log.info('RegisterUser.updateUserDispatcherQueue()\n'+str(msg))
      result = yield self.irc.update_user(msg)
      defer.returnValue(result)

   @defer.inlineCallbacks
   def updateUserEmail (self, msg):
      log.info('RegisterUser.updateUserDispatcherQueue()\n'+str(msg))
      result = yield self.irc.update_user(msg)
      defer.returnValue(result)

   @defer.inlineCallbacks
   def registerUser (self, msg):
      log.info('RegisterUser.registerUser()\n'+str(msg))
      result = yield self.irc.authenticate_user(msg.message_parameters_reference.certificate,
                                                msg.message_parameters_reference.rsa_private_key)
      if result == None:   # TODO: fix this so it works with IR (and fix IR)
         log.info('RegisterUser.registerUser(): user exists in IR\n'+str(result))
      else:
         result = yield self.irc.register_user(msg.message_parameters_reference.certificate,
                                               msg.message_parameters_reference.rsa_private_key)
         log.info('RegisterUser.registerUser(): added new user in IR\n'+str(result))
      msg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS RegisterUser response')
      msg.message_parameters_reference.add()
      msg.message_parameters_reference[0] = msg.CreateObject(OOI_ID_TYPE)
      msg.message_parameters_reference[0].ooi_id = result
      defer.returnValue(msg)



