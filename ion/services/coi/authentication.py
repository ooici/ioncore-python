#!/usr/bin/env python

"""
@file ion/services/coi/authentication_service.py
@author Roger Unwin
@brief service for registering and authenticating identities
"""


from M2Crypto import EVP, X509

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.core.exception import ReceivedError
from ion.test.iontest import IonTestCase
from ion.services.coi.identity_registry import IdentityRegistryClient

from ion.resources import coi_resource_descriptions





class Authentication():
    """
    """
    
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "authentication_service"
        ServiceClient.__init__(self, proc, **kwargs)
    
    def is_user_registered(self, user_cert, user_provate_key):
        """
        """
        return None
    
    def register_user(self, user_cert, user_provate_key):
        """
        """
        return None
    
    def authenticate_user(self, user_cert, user_provate_key):
        """
        """
        return None

    def sign_message(self, message, ooi_id):
        """
        
        """
        
        # user.certificate
        # user.rsa_private_key
        
        user = yield self.identity_registry_client.get_user(ooi_id)
        
        pkey = EVP.load_key_string(user.rsa_private_key)
        pkey.sign_init()
        pkey.sign_update(message)
        signature = pkey.sign_final()

        return signature

    def verify_message(self, message, ooi_id):
        """
        
        
        """
        user = yield self.identity_registry_client.get_user(ooi_id)
        
        x509 = X509.load_cert_string(user.certificate)
        pubkey = x509.get_pubkey()
        pubkey.verify_init()
        pubkey.verify_update(message)
        if pubkey.verify_final(sig) == 1:
            return True
        else:
            return False

    
    def encrypt_message(self, message, uuid):
        """
        """
        return None
    
    def decrypt_message(self, message, uuid):
        """
        """
        return None
    
    def verify_certificate_chain(self, certificate, key):
        """
        """
        return None
    
    def is_certificate_valid(self, user_cert, user_provate_key):
        """
        """
        return None
    
    def is_certificate_within_date_range(self, user_cert, user_provate_key):
        """
        """
        return None
   