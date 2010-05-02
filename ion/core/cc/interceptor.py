#!/usr/bin/env python

"""
@file ion/core/cc/interceptor.py
@author Michael Meisinger
@brief capability container interceptor system (security, governance)
"""

import logging
import hashlib
import json

from magnet.container import InterceptorSystem, Interceptor
from carrot.backends.base import BaseMessage
from ion.core import ioninit
import ion.util.procutils as pu

# Configuration
CONF = ioninit.config(__name__)
master_off = CONF.getValue('master_off', False)
encrypt = CONF.getValue('encrypt', False)
encrypt_mod = CONF.getValue('encrypt_mod', None)
encrypt_key = CONF.getValue('encrypt_key', None)
encrypt_pad = CONF.getValue('encrypt_pad', 16)

if encrypt:
    try:
        mod = pu.get_module(encrypt_mod)
        encrypter = mod.new(encrypt_key)
    except ImportError:
        encrypter = None
else:
    encrypter = None

class IdmInterceptor(Interceptor):
    """Message interceptor for identity management and security purposes.
    Called last before message hits the wire, and first after message received.
    """
    @classmethod
    def transform(cls, msg):
        """Identity management transform
        """
        if master_off: return msg
        try:
            if isinstance(msg, BaseMessage):
                msg = cls.message_validator(msg)
                if encrypt:
                    msg = MessageEncrypter.decrypt_message(msg)
            else:
                if encrypt:
                    msg = MessageEncrypter.encrypt_message(msg)
                msg = cls.message_signer(msg)
        except StandardError, e:
            pu.log_exception("IdM Interceptor failed",e)
        return msg

    @classmethod
    def message_validator(cls, msg):
        #logging.info('IdM interceptor IN')
        cont = msg.payload.copy()
        hashrec = cont.pop('signature')
        blob = json.dumps(cont, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        if hash != hashrec:
            logging.info("*********message signature wrong***********")
        return msg

    @classmethod
    def message_signer(cls, msg):
        #logging.info('IdM interceptor OUT')
        blob = json.dumps(msg, sort_keys=True)
        hash = hashlib.sha1(blob).hexdigest()
        msg['signature'] = hash
        return msg

class MessageEncrypter(object):
    @classmethod
    def encrypt_message(cls, msg):
        #logging.info("Encrypting message: "+str(msg))
        blob = json.dumps(msg, sort_keys=True)
        padding = int(((len(blob) + encrypt_pad) // encrypt_pad) * encrypt_pad)
        padmsg = blob.ljust(padding)
        #logging.info("Padded message json: '"+str(padmsg)+"'")
        encmsg = encrypter.encrypt(padmsg)
        logging.info("Encrypted message len="+str(len(encmsg)))
        # HACK1: Returning the encrypted message in a mutable dict so that
        # we can replce dict content when decoding
        # HACK2: Need to repr the binary encmsg because otherwise failure        
        return {'msg':repr(encmsg)}

    @classmethod
    def decrypt_message(cls, msg):
        # Note: modifying the dict in the msg.payload does not work
        encmsc = msg.payload.pop('msg')
        msgblob = encrypter.decrypt(eval(encmsc))
        #logging.info("Message decrypted: "+str(msgblob))
        msgobj = json.loads(msgblob)
        #logging.info("Message recreated: "+str(msgobj))
        msg.payload.update(msgobj)
        msg._decoded_cache = msgobj
        assert msgobj is msg.payload
        #logging.info("Message payload recreated: "+str(msg.payload))
        return msg
         
class PolicyInterceptor(Interceptor):
    @classmethod
    def transform(cls, msg):
        """Policy transform -- pass all
        """
        if master_off: return msg
        if isinstance(msg, BaseMessage):
            ""
            #logging.info('Policy interceptor IN')
        else:
            ""
            #logging.info('Policy interceptor OUT')
        return msg
    
class GovernanceInterceptor(Interceptor):
    @classmethod
    def transform(cls, msg):
        """Governance transform -- pass all
        """
        if master_off: return msg
        if isinstance(msg, BaseMessage):
            ""
            #logging.info('Governance interceptor IN')
        else:
            ""
            #logging.info('Governance interceptor OUT')
        return msg


class MessageEncoder(object):
    pass

class BaseInterceptorSystem(InterceptorSystem):
    """Custom capability container interceptor system for secutiry and
    governance purposes.
    """
    def __init__(self):
        InterceptorSystem.__init__(self)
        self.registerIdmInterceptor(IdmInterceptor.transform)
        self.registerPolicyInterceptor(PolicyInterceptor.transform)
        self.registerGovernanceInterceptor(GovernanceInterceptor.transform)
        logging.info("Initialized ION BaseInterceptorSystem")
