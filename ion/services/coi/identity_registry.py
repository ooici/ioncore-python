#!/usr/bin/env python

"""
@file ion/services/coi/identity_registry.py
@author Roger Unwin
@brief service for storing identities
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessFactory
#from ion.core.process.service_process import ServiceProcess
from ion.data import dataobject
from ion.data.datastore import registry
from ion.data.datastore.registry import  BaseRegistryClient,  BaseRegistryService
#import uuid
from ion.core import ioninit
#import re
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.resources import coi_resource_descriptions 
from ion.services.coi.authentication import Authentication

CONF = ioninit.config(__name__)



class IdentityRegistryClient(BaseRegistryClient):
    """
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "identity_service"
        ServiceClient.__init__(self, proc, **kwargs)
        


    def clear_identity_registry(self):
        return self.base_clear_registry('clear_identity_registry')

    def register_user(self, user):
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
    
    @defer.inlineCallbacks
    def register_user_credentials(self, user_cert, user_private_key):
        print "GOT HERE"
        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key,
        }
        
        (content, headers, msg) = yield self.rpc_send('register_user_credentials', cont)
        
        log.info('Service reply: ' + str(content))

        defer.returnValue(str(content))


        

class IdentityRegistryService(BaseRegistryService):

     # Declaration of service
    declare = ServiceProcess.service_declare(name='identity_service', version='0.1.0', dependencies=[])

    op_clear_identity_registry = BaseRegistryService.base_clear_registry
    op_register_user = BaseRegistryService.base_register_resource
    op_update_user = BaseRegistryService.base_register_resource
    op_get_user = BaseRegistryService.base_get_resource
    op_set_identity_lcstate = BaseRegistryService.base_set_resource_lcstate
    op_find_users = BaseRegistryService.base_find_resource
    
    @defer.inlineCallbacks
    def op_register_user_credentials(self, content, headers, msg):
        """
        Need to migrate the three functions from authentication to here. here is a sample of how a function as a serivce works.  taken from exchange_management
        """
        """
        the subject field from the cert should be the users working unique name.
        """

        authentication = Authentication()
        cert_info = authentication.decode_certificate(content['user_cert'])
        print cert_info['subject']
        
        user = coi_resource_descriptions.IdentityResource.create_new_resource()
        user.certificate = content['user_cert']
        user.private_key = content['user_private_key']
        
        cert_details = authentication.decode_certificate(content['user_cert'])
        user.subject = cert_details['subject']
        
        #bogus line
        
        #user = yield self.op_register_user(user)
        #user1 = self.base_register_resource('register_user', user)
        (content, headers, msg) = yield self.rpc_send('register_user', user)
        
        ooi_id = user1.reference().RegistryIdentity
        
        # how to go from ooid back to a field that works for get_user
        # if cant, then might have to use find user.
        #ResourceReference(RegistryIdentity=ooi_id)
        #r_ref = ResourceReference(RegistryIdentity=ooi_id)
        #user0 = yield self.identity_registry_client.get_user(r_ref)
        
        
        print "got subject back" + str(user.subject)


        yield self.reply_ok(msg, ooi_id)




# Spawn of the process using the module name
factory = ProcessFactory(IdentityRegistryService)
