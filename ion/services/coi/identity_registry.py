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
        

    def ooi_id_to_user_ref(self, ooi_id):
        ResourceReference(RegistryIdentity=ooi_id)
        user_ref = ResourceReference(RegistryIdentity=ooi_id)
        return user_ref

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
        """
        This registers a user by storing the user certificate, user private key, and certificate subject line(derived from the certificate)
        It returns a ooi_id which is the uuid of the record and can be used to uniquely identify a user.
        """
        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key,
        }
        
        (content, headers, msg) = yield self.rpc_send('register_user_credentials', cont)
        
        defer.returnValue(str(content))
        
    @defer.inlineCallbacks
    def is_user_registered(self, user_cert, user_private_key):
        """
        This determines if a user is registered by deriving the subject line from the certificate and scanning the registry for that line.
        It returns True or False
        """
        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key,
        }
        
        (content, headers, msg) = yield self.rpc_send('verify_registration', cont)
        
        defer.returnValue( content )
        
    @defer.inlineCallbacks
    def authenticate_user(self, user_cert, user_private_key):
        """
        This authenticates that the user exists. If so, the credentials are replaced with the current ones, and a ooi_id is returned. If not, None is returned.
        """
        cont = {
            'user_cert': user_cert,
            'user_private_key': user_private_key,
        }
        
        (content, headers, msg) = yield self.rpc_send('authenticate_user_credentials', cont)
        
        defer.returnValue( content )

        

        
        

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
        This registers a user by storing the user certificate, user private key, and certificate subject line(derived from the certificate)
        It returns a ooi_id which is the uuid of the record and can be used to uniquely identify a user.
        """

        authentication = Authentication()
        cert_info = authentication.decode_certificate(content['user_cert'])
        
        user = coi_resource_descriptions.IdentityResource.create_new_resource()
        user.certificate = content['user_cert']
        user.private_key = content['user_private_key']
        
        cert_details = authentication.decode_certificate(content['user_cert'])
        user.subject = cert_details['subject']
        
        registered_user = yield self.reg.register_resource(user) # This is performing this... op_regiser_user(user)
        ooi_id = registered_user.reference().RegistryIdentity
        yield self.reply_ok(msg, ooi_id)
        

    @defer.inlineCallbacks
    def op_verify_registration(self, content, headers, msg):
        """
        This determines if a user is registered by deriving the subject line from the certificate and scanning the registry for that line.
        It returns True or False
        """
        authentication = Authentication()
        cert_info = authentication.decode_certificate(content['user_cert'])
        
        user_description = coi_resource_descriptions.IdentityResource()
        user_description.subject = cert_info['subject']
        
        users = yield self.reg.find_resource(user_description, regex=False) # This is performing this... op_find_user(user)
        
        if len(users) > 0:
            yield self.reply_ok(msg, True)
        else:
            yield self.reply_ok(msg, False)

    @defer.inlineCallbacks
    def op_authenticate_user_credentials(self, content, headers, msg):
        """
        This authenticates that the user exists. If so, the credentials are replaced with the current ones, and a ooi_id is returned. If not, None is returned.
        """
        
        authentication = Authentication()
        cert_info = authentication.decode_certificate(content['user_cert'])
        
        user_description = coi_resource_descriptions.IdentityResource()
        user_description.subject = cert_info['subject']
        
        users = yield self.reg.find_resource(user_description, regex=False) # This is performing this... op_find_user(user)
        #print len(users)
        if users[0].subject == cert_info['subject']:
            users[0].certificate = content['user_cert']
            users[0].private_key = content['user_private_key']
            registered_user = yield self.reg.register_resource(users[0]) # This is performing this... op_update_user(user)
            ooi_id = registered_user.reference().RegistryIdentity
            
            yield self.reply_ok(msg, ooi_id)
        else:
            yield self.reply_ok(msg, None)
        
        
        
        

# Spawn of the process using the module name
factory = ProcessFactory(IdentityRegistryService)
