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


class IdentityRegistryServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "register_user"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def register_user(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('register_user', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)
        
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
    def authenticate(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('authenticate', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)   

    @defer.inlineCallbacks   
    def generate_ooi_id(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('generate_ooi_id', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def revoke_ooi_id(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('revoke_ooi_id', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def store_registration(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('store_registration', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)        
        
    @defer.inlineCallbacks
    def store_registration_info(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('store_registration_info', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def get_registration_info(self, text='Testing'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_registration_info', text)
        logging.info('### Service reply: '+str(content))
        defer.returnValue(content)        

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
        
class IdentityRegistryService(BaseService):
    """(User and resource) identity registry service interface
    """
    
    # Declaration of service
    declare = BaseService.service_declare(name='register_user', version='0.1.0', dependencies=[])
    
    
    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('### IdentityRegistryService.__init__()')
        
    @defer.inlineCallbacks    
    def op_define_identity(self, content, headers, msg):
        """Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})
        
    @defer.inlineCallbacks
    def op_register_user(self, content, headers, msg):
        """Service operation: .
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
    def op_authenticate(self, content, headers, msg):
        """Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})

    """
    Begin experimental methods RU
    """
    @defer.inlineCallbacks 
    def op_generate_ooi_id(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})

    @defer.inlineCallbacks 
    def op_revoke_ooi_id(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})
    
    @defer.inlineCallbacks 
    def op_store_registration(self, content, headers, msg):
        """SRU ervice operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})

    @defer.inlineCallbacks 
    def op_store_registration_info(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})

    @defer.inlineCallbacks 
    def op_get_registration_info(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})

    @defer.inlineCallbacks 
    def op_update_registration_info(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})
        
    @defer.inlineCallbacks 
    def op_revoke_registration(self, content, headers, msg):
        """RU Service operation: .
        """
        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'op_register_user ******RETURNING: '+str(content)}, {})
        
        
        
        
        
# Spawn of the process using the module name
factory = ProtocolFactory(IdentityRegistryService)

