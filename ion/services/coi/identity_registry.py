#!/usr/bin/env python

"""
@file ion/services/coi/identity_registry.py
@author Michael Meisinger
@brief service for registering and authenticating identities
"""

import logging
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.data import dataobject
from ion.data.datastore import registry
from ion.services.coi import resource_registry

import uuid # experimental. remember to remove after done playing with uuid
from ion.core import ioninit

CONF = ioninit.config(__name__)

class Person(registry.ResourceDescription):
    """
    Need to pull this out to its own file.  but not yet....
    """
   
    # These are the fields that we get from the Trust Provider
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






class IdentityRegistryServiceClient(BaseServiceClient): # inherit from BaseRegistry Client # gives me lc stuff and find
    
    # test_resource_registry is my friend.
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        """
        """
        self.ResourceRegistryClient = resource_registry.ResourceRegistryClient(proc, **kwargs)

    @defer.inlineCallbacks
    def register_user(self, parms):
        
        # @bug Possible bug. May need to test if the user is already registered, or collides with another user.
        
        # copy data to object
        # save object
        # find object
        # pull field from object
        
        res_id = str(uuid.uuid3(uuid.uuid4(), parms['trust_provider'])) #"user_registration_store" # this is actually supposed to be a unique id for the person
        
        # create a new user
        user = Person()
        
        # initialize the user
        user.common_name = parms['common_name']
        user.country = parms['country'] 
        user.trust_provider = parms['trust_provider']
        user.domain_component = parms['domain_component']
        user.certificate = parms['certificate']
        user.rsa_private_key = parms['rsa_private_key']
        user.expiration_date = parms['expiration_date']
        # These are the fields we prompt the user for during registration
        user.first_name = parms['first_name']
        user.last_name = parms['last_name']
        user.phone = parms['phone']
        user.fax = parms['fax']
        user.email = parms['email']
        user.organization = parms['organization']
        user.department = parms['department']
        user.title = parms['title']

        # store user
        yield self.ResourceRegistryClient.register_resource(res_id, user)
        
        # do I need to call the service? can i migrate this to the service?
       
        defer.returnValue(res_id)
        
    @defer.inlineCallbacks
    def update_user(self, parms):
        """
        Takes dict of values.  ooi_id is defined in the dict of values.
        
        This should load the user, then set any values in the dict...
        """
        res_id = parms['ooi_id']
        # create a new user
        user = Person()
        
        old_user = yield self.ResourceRegistryClient.get_resource(res_id)
        
        old_user_dict = old_user.__dict__
        for key in list(old_user_dict):
            if key in list(user.__dict__):
                if isinstance(old_user_dict[key], basestring):
                    setattr(user, key[1:], old_user_dict[key])
                    logging.info("###--> update_user setting old value for " + key[1:] + " = " + old_user_dict[key] + " for user.")
                
        
        for key in list(parms):
            setattr(user, key, parms[key])
            logging.info("###--> update_user setting new value for " + key + " = " + parms[key] + " for user.")
        
    
        # store user
        
        yield self.ResourceRegistryClient.register_resource(res_id, user)
        
        # do I need to call the service? can i migrate this to the service?
       
        defer.returnValue(res_id)
        
 
        
    @defer.inlineCallbacks   
    def define_user_profile(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('define_user_profile', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)  
        
    @defer.inlineCallbacks   
    def define_identity(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('define_identity', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)      

    @defer.inlineCallbacks   
    def define_user_profile(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('define_user_profile', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)    




    @defer.inlineCallbacks   
    def authenticate(self, parms):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('authenticate', parms)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)   





    @defer.inlineCallbacks   
    def generate_ooi_id(self, text='Testing'):
        
        # in is email
        
        # find all matches for email
        
        
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('generate_ooi_id', text)
        logging.info('### Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def revoke_ooi_id(self, parms):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('revoke_ooi_id', parms)
        logging.info('### Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def store_registration(self, parms):
        yield self._check_init()
        #(content, headers, msg) = yield self.rpc_send('store_registration', parms)
        #logging.info('### Service reply: '+str(content))
        defer.returnValue({'value':None})        
        
    @defer.inlineCallbacks
    def store_registration_info(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('store_registration_info', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def get_registration_info(self, ooi_id):
        
        
        res_id = ooi_id #"user_registration_store" # this is actually supposed to be a unique id for the person
     
        foo = yield self.ResourceRegistryClient.get_resource(res_id)
        
        # @bug will fail bad if resid does not exist
        if not foo:
            defer.returnValue(foo)
        else :
            defer.returnValue(foo.phone)
        
        
    @defer.inlineCallbacks
    def update_registration_info(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('update_registration_info', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)            

    @defer.inlineCallbacks
    def revoke_registration(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('revoke_registration', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)
        
        
        
    '''    
        find_resource / op_find_resource pass in attribute and value
        need op_register_resource op_register_identity # similar to his
        op_get_resource op_get_identity # similar to his
    '''
    
class IdentityRegistryService(BaseService):  # should inherit from BaseResourceService in coi resource registry
    """(User and resource) identity registry service interface
    """
    
    # Declaration of service
    declare = BaseService.service_declare(name='register_user', version='0.1.0', dependencies=[])
    
    #@defer.inlineCallbacks     
    #def slc_init(self):
    #    """
    #    """
    #    # initialize data store
    #    #self.rdfs=RdfStore()
    #    #yield self.rdfs.init()
    '''
        
    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('### IdentityRegistryService.__init__()')
        
        
        
        
    @defer.inlineCallbacks    
    def op_define_identity(self, content, headers, msg):
        """Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_define_identity ******RETURNING: '+str(content)}, {})
        
    @defer.inlineCallbacks
    def op_register_user(self, content, headers, msg):
        """Service operation: .
        """
        
        (content, headers, msg) = yield self.rpc_send('register_resource',
                                            {'res_id':res_id,'res_enc':resource.encode()})
        
        
        """
        self.rrc = ResourceRegistryClient(proc=sup)
        
        user_id = "ooi3127"
        person_object = Person()
        person_object.first_name = 'Roger'
        
        rid = yield self.rrc.register_resource(str(user_id), person_object) 
        logging.info('Resource registered with id '+str(rid))
    
        rd3 = yield self.rrc.get_resource(rid)
        """
        
        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})

    @defer.inlineCallbacks 
    def op_define_user_profile(self, content, headers, msg):
        """Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_define_user_profile ******RETURNING: '+str(content)}, {})
        
    @defer.inlineCallbacks         
    def op_authenticate(self, parms, headers, msg):
        """ Service operation: need to take values from parms and verify they exist in the data store.
        """
        
        
        yield self.reply_ok(msg, {'authenticated': True}, {})


    """
    Begin experimental methods RU
    """
    @defer.inlineCallbacks 
    def op_generate_ooi_id(self, content, headers, msg):
        """ Service operation: this should generate a unique id when called.  Depending on if its user viewable or not
            will determine if it needs to be based on their user name.  At this point i am not decided on how it should
            be generated.
        """
        
        
        
        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'ooi_id': 1231231123}, {})

    @defer.inlineCallbacks 
    def op_revoke_ooi_id(self, parms, headers, msg):
        """RU Service operation: sormat for inputs.
           parms = {'ooi_id':'username'}
           
           need to search the data store for the ooi_id and if present, flag it as revoked. then return true. return false on falure to find it?
        
        """

        
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'revoked': True}, {})
    
    
    
    
    
    @defer.inlineCallbacks 
    def op_store_registration(self, parms, headers, msg):
        """store retistration service operation:
            parms = {'common_name': 'Roger Unwin A136',
                 'organization': 'ProtectNetwork',
                 'Domain Component': 'cilogon org',
                 'Country': 'US',
                 'Certificate': 'dummy certificate',
                 'RSA Private Key': 'dummy rsa private key'}
                 
                 
            the params should be stored, but it seems that mechanism is not completely done yet. so defer.
        """
        yield self.reply_ok(msg, {'value':'op_get_registration_info ******RETURNING: '+str(parms)}, {})







    @defer.inlineCallbacks 
    def op_store_registration_info(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_store_registration_info ******RETURNING: '+str(content)}, {})

    @defer.inlineCallbacks 
    def op_get_registration_info(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_get_registration_info ******RETURNING: '+str(content)}, {})

    @defer.inlineCallbacks 
    def op_update_registration_info(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_update_registration_info ******RETURNING: '+str(content)}, {})
        
    @defer.inlineCallbacks 
    def op_revoke_registration(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_revoke_registration ******RETURNING: '+str(content)}, {})
        
        
        
        
        
        
        
        
        


    '''
# Spawn of the process using the module name
factory = ProtocolFactory(IdentityRegistryService)



