#!/usr/bin/env python

"""
@file ion/integration/ais/RegisterUser/RegisterUser.py
@author Bill Bollenbcher
@brief The worker class that implements the RegisterUser function for the AIS  (workflow #107)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.integration.ais.ais_object_identifiers import UPDATE_USER_TYPE, UPDATE_USER_DISPATCH_QUEUE_TYPE

class RegisterUser():
    
   def init(self, ais):
      log.info('RegisterUser.init()')
      self.irc = IdentityRegistryClient(proc=ais)
        
   @defer.inlineCallbacks
   def updateUserDispatcherQueue (self, msg):
      log.info('RegisterUser.updateUserDispatcherQueue()\n'+str(msg))
      result = yield self.irc.update_user(msg)
      defer.returnValue(result)

   @defer.inlineCallbacks
   def updateUser (self, msg):
      log.info('RegisterUser.updateUser()\n'+str(msg))
      result = yield self.irc.update_user(msg)
      defer.returnValue(result)

   @defer.inlineCallbacks
   def registerUser (self, msg):
      log.info('RegisterUser.registerUser()\n'+str(msg))
      result = yield self.irc.register_user(msg.message_parameters_reference.certificate, msg.message_parameters_reference.rsa_private_key)
      defer.returnValue(result)



