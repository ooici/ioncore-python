#!/usr/bin/env python

"""
@file ion/services/coi/identity_registry.py
@author Roger Unwin
@brief service for registering and authenticating identities
"""

import logging
logging = logging.getLogger(__name__)

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


'''
# Moved resource definition to coi_resource_descriptions
class Person(registry.ResourceDescription): 
    """
    Need to pull this out to its own file.  but not yet....
    """

    # These are the fields that we get from the Trust Provider
    ooi_id = dataobject.TypedAttribute(str)
    common_name = dataobject.TypedAttribute(str)
    country = dataobject.TypedAttribute(str)
    trust_provider = dataobject.TypedAttribute(str) # this is the trust provider /O (Organization field)
    domain_component = dataobject.TypedAttribute(str)
    certificate = dataobject.TypedAttribute(str)
    rsa_private_key = dataobject.TypedAttribute(str)
    expiration_date = dataobject.TypedAttribute(str)
    # These are the fields we prompt the user for during registration
    first_name = dataobject.TypedAttribute(str)
    last_name = dataobject.TypedAttribute(str)
    phone = dataobject.TypedAttribute(str)
    fax = dataobject.TypedAttribute(str)
    email = dataobject.TypedAttribute(str)
    organization = dataobject.TypedAttribute(str)
    department = dataobject.TypedAttribute(str)
    title = dataobject.TypedAttribute(str)

    # address?

class IdentityRegistryClient(BaseRegistryClient):
    """
    This service is responsible for registering new users, and retrieving user information.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "identity_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def register_user(self, user):
        """
        Needs documentation here
        """
        
        user.ooi_id = str(uuid.uuid4())
        
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('register_user', {'user_enc': user.encode()})
        
        defer.returnValue(str(content['user_id']))

    @defer.inlineCallbacks
    def find_users(self, attributes):
        """
        @brief Retrieve all the Person(s) in the registry
        @param attributes is a dictionary of attributes which will be used to select a resource
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('find_users', attributes)
        
        users_enc = content['users_enc']
        users=[]
        if users_enc != None:
            for user in users_enc:
                users.append(registry.ResourceDescription.decode(user)())
        defer.returnValue(users)


    @defer.inlineCallbacks
    def get_user(self, user_id):
        """
        @brief Retrieve a resource from the registry by its ID
        @param res_id is a resource identifier unique to this resource
        """

        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_user', {'user_id': user_id})
        logging.info('Service reply: '+str(content))
        user_enc = content['user_enc']

        if user_enc != None:
            user = registry.ResourceDescription.decode(user_enc)()
            defer.returnValue(user)
        else:
            defer.returnValue(None)

    @defer.inlineCallbacks
    def update_user(self, Person):
        """
        This one needs a comment
        """
        
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('register_user', {'user_id': Person.ooi_id, 'user_enc': Person.encode()})
        
        defer.returnValue(str(content['user_id']))
'''

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


'''
class IdentityRegistryService(BaseResourceRegistryService):  # (was BaseService) should inherit from BaseResourceService in coi resource registry
    """
    Identity registry service interface
    """

    # Declaration of service
    declare = BaseResourceRegistryService.service_declare(name='identity_registry', version='0.1.0', dependencies=[])

    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)


    @defer.inlineCallbacks
    def op_find_users(self, content, headers, msg):
        """
        @brief : Service operation: Find users by criteria.
        @param : content: a dictionary of attributes which must match a resource

        This will change in the future once the class we inherit from makes this a generic
        """

        users_list = yield self.reg.list_descriptions()
        find_list=[]
        for user in users_list:
            # Test for failure and break
            test=True
            for k, v in content.items():
                # if this resource does not contain this attribute move on
                if not k in user.attributes:
                    test = False
                    break

                att = getattr(user, k, None)

                # Bogus - can't send lcstate objects in a dict must convert to sting to test
                if isinstance(att, registry.LCState):
                    att = str(att)

                if isinstance(v, (str, unicode) ):
                    # Use regex
                    if not re.search(v, att):
                        test=False
                        break
                else:
                    # test equality
                    #@TODO add tests for range and in list...


                    if att != v and v != None:
                       test=False
                       break
            if test:
                find_list.append(user)

        yield self.reply_ok(msg, {'users_enc':list([user.encode() for user in find_list])} )

    @defer.inlineCallbacks
    def op_register_user(self, content, headers, msg):
        """
        @brief : Register a user instance with the user registry.
        @param : User object (encoded)
        """
        
        user_enc = content['user_enc']
        user = registry.ResourceDescription.decode(user_enc)()
        
        yield self.reg.register(user.ooi_id, user)
        yield self.reply_ok(msg, {'user_id':user.ooi_id},)

    @defer.inlineCallbacks
    def op_get_user(self, content, headers, msg):
        """
        @brief : Service operation: Get a user info record.
        @param : user_id: the ooi_id for the user (uuid4())
        """
        user_id = content['user_id']
        
        user = yield self.reg.get_description(user_id)
        if user:
            yield self.reply_ok(msg, {'user_enc': user.encode()})
        else:
            yield self.reply_err(msg, {'user_enc': None})
'''




# Spawn of the process using the module name
factory = ProtocolFactory(IdentityRegistryService)
