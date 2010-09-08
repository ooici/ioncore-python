#!/usr/bin/env python

"""
@file ion/services/coi/identity_registry.py
@author Roger Unwin
@brief service for registering and authenticating identities
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.base_process import ProtocolFactory
#from ion.services.base_service import BaseService
from ion.data import dataobject
from ion.data.datastore import registry
from ion.data.datastore.registry import  BaseRegistryClient,  BaseRegistryService
#import uuid
from ion.core import ioninit
#import re
from ion.services.base_service import BaseService, BaseServiceClient

from ion.resources import coi_resource_descriptions 

CONF = ioninit.config(__name__)



class IdentityRegistryClient(BaseRegistryClient):
    """
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "identity_service"
        BaseServiceClient.__init__(self, proc, **kwargs)


    def clear_identity_registry(self):
        return self.base_clear_registry('clear_identity_registry')

    def register_user(self,user):
        return self.base_register_resource('register_user', user)
        
    def update_user(self, user):
        return self.base_register_resource('update_user', user)

    def get_user(self,user_reference):
        return self.base_get_resource('get_user', user_reference)
        
    def set_identity_lcstate(self, identity_reference, lcstate):
        return self.base_set_resource_lcstate('set_identity_lcstate',identity_reference, lcstate)

    def find_users(self, user_description,regex=True,ignore_defaults=True, attnames=[]):
        return self.base_find_resource('find_users',user_description,regex,ignore_defaults,attnames)

    def set_identity_lcstate_new(self, identity_reference):
        return self.set_identity_lcstate(identity_reference, dataobject.LCStates.new)

    def set_identity_lcstate_active(self, identity_reference):
        return self.set_identity_lcstate(identity_reference, dataobject.LCStates.active)
        
    def set_identity_lcstate_inactive(self, identity_reference):
        return self.set_identity_lcstate(identity_reference, dataobject.LCStates.inactive)

    def set_identity_lcstate_decomm(self, identity_reference):
        return self.set_identity_lcstate(identity_reference, dataobject.LCStates.decomm)

    def set_identity_lcstate_retired(self, identity_reference):
        return self.set_identity_lcstate(identity_reference, dataobject.LCStates.retired)

    def set_identity_lcstate_developed(self, identity_reference):
        return self.set_identity_lcstate(identity_reference, dataobject.LCStates.developed)

    def set_identity_lcstate_commissioned(self, identity_reference):
        return self.set_identity_lcstate(identity_reference, dataobject.LCStates.commissioned)

class IdentityRegistryService(BaseRegistryService):

     # Declaration of service
    declare = BaseService.service_declare(name='identity_service', version='0.1.0', dependencies=[])

    op_clear_identity_registry = BaseRegistryService.base_clear_registry
    op_register_user = BaseRegistryService.base_register_resource
    op_update_user = BaseRegistryService.base_register_resource
    op_get_user = BaseRegistryService.base_get_resource
    op_set_identity_lcstate = BaseRegistryService.base_set_resource_lcstate
    op_find_users = BaseRegistryService.base_find_resource






# Spawn of the process using the module name
factory = ProtocolFactory(IdentityRegistryService)
