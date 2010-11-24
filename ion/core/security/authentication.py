#!/usr/bin/env python

"""
@file ion/core/security/authentication.py
@author Roger Unwin
@author Dorian Raymer
@brief Functionality for registering and authenticating identities
"""

import binascii
import urllib
import os
import sys
import tempfile
import datetime
import calendar
import time

from M2Crypto import EVP, X509, BIO, SMIME, RSA

from twisted.internet import defer

import ion.util.ionlog
#from ion.resources import coi_resource_descriptions 


log = ion.util.ionlog.getLogger(__name__)

#XXX @note What is this?
sys.path.insert(0, "build/lib.linux-i686-2.4/")

#XXX @todo Fix: Should not need absolute paths.
BASEPATH = os.path.realpath(".")
CERTIFICATE_PATH = BASEPATH + '/res/certificates/'

class Authentication(object):
    """
    """

    def Xis_user_registered(self, user_cert, user_private_key):
        """
        the subject field from the cert should be the users working unique name.
        """
        cert_info = self.decode_certificate(user_cert)
        print cert_info['subject']
        return None

    @defer.inlineCallbacks
    def Xregister_user(self, user_cert, user_private_key):
        """
        the subject field from the cert should be the users working unique name.
        """
        cert_info = self.decode_certificate(user_cert)
        print cert_info['subject']
        
        #user = coi_resource_descriptions.IdentityResource.create_new_resource()
        user.certificate = user_cert
        user.private_key = user_private_key
        
        cert_details = self.decode_certificate(user_cert)
        user.subject = cert_details['subject']
        user = yield self.identity_registry_client.register_user(user)

        ooi_id = user.reference().RegistryIdentity
        
        # how to go from ooid back to a field that works for get_user
        # if cant, then might have to use find user.
        ResourceReference(RegistryIdentity=ooi_id)
        r_ref = ResourceReference(RegistryIdentity=ooi_id)
        user0 = yield self.identity_registry_client.get_user(r_ref)
        print "got subject back" + str(user.subject)
        defer.returnValue(ooi_id)

    def Xauthenticate_user(self, user_cert, user_private_key):
        """
        the subject field from the cert should be the users working unique name.
        """
        cert_info = self.decode_certificate(user_cert)
        print cert_info['subject']
        return None


    def sign_message_hex(self, message, rsa_private_key):
        """
        @param message byte string
        return a hex encoded signature for a message
        """
        return binascii.hexlify(self.sign_message(message, rsa_private_key))

    def sign_message(self, message, rsa_private_key): 
        """
        take a message, and return a binary signature of it
        """
        pkey = EVP.load_key_string(rsa_private_key)
        pkey.sign_init()
        pkey.sign_update(message)
        sig = pkey.sign_final()
        return sig

    def verify_message_hex(self, message, certificate, signed_message_hex):
        """
        verify a hex encoded signature for a message
        """
        return self.verify_message(message, certificate, binascii.unhexlify(signed_message_hex))

    def verify_message(self, message, certificate, signed_message):
        """
        """
        x509 = X509.load_cert_string(certificate)
        pubkey = x509.get_pubkey()
        pubkey.verify_init()
        pubkey.verify_update(message)
        if pubkey.verify_final(signed_message) == 1:
            return True
        else:
            return False

    def private_key_encrypt_message_hex(self, message, private_key):
        return binascii.hexlify(self.private_key_encrypt_message(message, private_key))

    def private_key_encrypt_message(self, message, private_key):
        """
        """
        priv = RSA.load_key_string(private_key)
        
        p = getattr(RSA, 'pkcs1_padding')
        ctxt = priv.private_encrypt(message, p)
        return ctxt

    def private_key_decrypt_message_hex(self, encrypted_message, private_key):
        return self.private_key_decrypt_message(binascii.unhexlify(encrypted_message), private_key)

    def private_key_decrypt_message(self, encrypted_message, private_key):
        """
        """
        priv = RSA.load_key_string(private_key)
        p = getattr(RSA, 'pkcs1_padding')
        ptxt = priv.public_decrypt(encrypted_message, p)
        return ptxt

    def public_encrypt_hex(self, message, private_key):
        return binascii.hexlify(self.public_encrypt(message, private_key))

    def public_encrypt(self, message, private_key):
        """
        this encrypts messages that will be decrypted using private_decrypt
        """
        priv = RSA.load_key_string(private_key)
      
        p = getattr(RSA, 'pkcs1_padding') # can be either 'pkcs1_padding', 'pkcs1_oaep_padding'
        ctxt = priv.public_encrypt(message, p)
        
        return ctxt

    def private_decrypt_hex(self, encrypted_message, private_key):
        return self.private_decrypt(binascii.unhexlify(encrypted_message), private_key)

    def private_decrypt(self, encrypted_message, private_key):
        """
        this decrypts messages encrypted using public_encrypt
        """
        
        priv = RSA.load_key_string(private_key)
        
        p = getattr(RSA, 'pkcs1_padding') # can be either 'pkcs1_padding', 'pkcs1_oaep_padding'
        ptxt = priv.private_decrypt(encrypted_message, p)
        
        return ptxt

    def decode_certificate(self, certificate):
        """
        Return a Dict of all known attributes for the certificate
        """
        attributes = {}
        x509 = X509.load_cert_string(certificate, format=1)
        
        attributes['subject_items'] = {}
        attributes['subject'] = str(x509.get_subject())
        for item in attributes['subject'].split('/'):
            try:
                key,value = item.split('=')
                attributes['subject_items'][key] = urllib.unquote(value)
            except:
                """
                """
        
        attributes['issuer_items'] = {}
        attributes['issuer'] = str(x509.get_issuer())
        for item in attributes['issuer'].split('/'):
            try:
                key,value = item.split('=')
                attributes['issuer_items'][key] = urllib.unquote(value)
            except:
                """
                """
        
        attributes['not_valid_before'] = str(x509.get_not_before())
        attributes['not_valid_after'] = str(x509.get_not_after())
        attributes['ext_count'] = str(x509.get_ext_count())
        attributes['fingerprint'] = str(x509.get_fingerprint())
        attributes['text'] = str(x509.as_text())
        attributes['serial_number'] = str(x509.get_serial_number())
        attributes['version'] = str(x509.get_version())
        
        print str(attributes)
        return attributes

    def is_certificate_descended_from(self, user_cert, ca_file_name):
        store = X509.X509_Store()
        store.add_x509(X509.load_cert(CERTIFICATE_PATH + ca_file_name))
        x509 = X509.load_cert_string(user_cert)
        return store.verify_cert(x509)

    def is_certificate_valid(self, user_cert):
        return self.verify_certificate_chain(user_cert)

    def verify_certificate_chain(self, user_cert):
        """
       
        """
        #cilogon-basic.pem	cilogon-openid.pem	cilogon-silver.pem
        validity = False
        for ca_file_name in ['cilogon-basic.pem', 'cilogon-openid.pem', 'cilogon-silver.pem']:
            if self.is_certificate_descended_from(user_cert, ca_file_name) == 1:
                validity = True
                
        return validity

    def get_certificate_level(self, user_cert):
        """
        return what level of trust the certificate comes with
        """
        if self.is_certificate_descended_from(user_cert, 'cilogon-openid.pem'):
            return 'Openid'
        if self.is_certificate_descended_from(user_cert, 'cilogon-basic.pem'):
            return 'Basic'
        if self.is_certificate_descended_from(user_cert, 'cilogon-silver.pem'):
            return 'Silver'
        return 'Invalid'

    def is_certificate_within_date_range(self, user_cert):
        """
        Test if the current date is covered by the certificates valid within date range.
        """
        os.environ['TZ'] = 'GMT'
        time.tzset()

        cert = X509.load_cert_string(user_cert)
        nvb = datetime.datetime.strptime(str(cert.get_not_before()),"%b %d %H:%M:%S %Y %Z")
        nva = datetime.datetime.strptime(str(cert.get_not_after()),"%b %d %H:%M:%S %Y %Z")
        today = datetime.datetime.today()
        
        if today < nvb:
            return False
        if today > nva:
            return False
        return True

