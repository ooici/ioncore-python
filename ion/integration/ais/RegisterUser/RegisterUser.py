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
    
   def __init__(self, *args, **kwargs):
      self.irc = IdentityRegistryClient()
      log.info('RegisterUser.__init__()')
        
   def updateUserDispatcherQueue ():
      log.info('RegisterUser.UpdateUserDispatcherQueue()')

   def updateUser (msg):
      log.info('RegisterUser.UpdateUser()')



