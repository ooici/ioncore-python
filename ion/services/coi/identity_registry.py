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

logging.debug('!!!!!!!!!!!!!!!!!!STARTED!!!!!!!!!!!!!!!!!')

class IdentityRegistryServiceClient(BaseServiceClient):
    """
    This is an exemplar service client that calls the hello service. It
    makes service calls RPC style.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "register_user"
            logging.info('!!!!!!!!!!!!!!!!!!!!!!!!!targetname not in kargs!!!!!!!!!!!!!!!!!!!!!!!!!')
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def register_user(self, text='Hi there'):
        logging.debug('+++++++++++++++++GOT HERE 1')
        yield self._check_init()
        logging.debug('+++++++++++++++++GOT HERE 2')
        (content, headers, msg) = yield self.rpc_send('register_user', text)
        logging.debug('+++++++++++++++++GOT HERE 2')
        logging.info('#########################Service reply: '+str(content))
        defer.returnValue(str(content))
        
        
        

class IdentityRegistryService(BaseService):
    """(User and resource) identity registry service interface
    """
    def __init__(self, receiver, spawnArgs=None):
        # Service class initializer. Basic config, but no yields allowed.
        BaseService.__init__(self, receiver, spawnArgs)
        logging.info('IdentityRegistryService.__init__()')
        logging.debug('+++++++++++++++++IdentityRegistryService __init__ called.')
        
    # Declaration of service
    declare = BaseService.service_declare(name='register_user', version='0.1.0', dependencies=[])
    
    def op_define_identity(self, content, headers, msg):
        """Service operation: .
        """

    def op_register_user(self, content, headers, msg):
        """Service operation: .
        """
        logging.debug('GOT HERE 11 !!!!')

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, {'value':'Hello there, '+str(content)}, {})
        logging.debug('GOT HERE 22 !!!!')

    def op_define_user_profile(self, content, headers, msg):
        """Service operation: .
        """

    def op_authenticate(self, content, headers, msg):
        """Service operation: .
        """

    """
    Begin experimental methods RU
    """
    def op_generate_ooi_id(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_revoke_ooi_id(self, content, headers, msg):
        """ RU Service operation: .
        """
    
    def op_store_registration(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_store_registration_info(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_get_registration_info(self, content, headers, msg):
        """ RU Service operation: .
        """

    def op_update_registration_info(self, content, headers, msg):
        """ RU Service operation: .
        """
        
    def op_revoke_registration(self, content, headers, msg):
        """ RU Service operation: .
        """
        
        
        
        
        
# Spawn of the process using the module name
factory = ProtocolFactory(IdentityRegistryService)

